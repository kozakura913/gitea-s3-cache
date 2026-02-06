
use std::{collections::HashMap, net::SocketAddr, pin::Pin, sync::Arc};

use aws_sdk_s3::{primitives::ByteStream, types::{CompletedMultipartUpload, CompletedPart, Object}};
use aws_smithy_types::body::SdkBody;
use axum::{Router, extract::{Path, State}, http::StatusCode, response::IntoResponse};
use futures::StreamExt;
use headers::{ContentRange, Header};
use redis::{AsyncCommands, aio::ConnectionManagerConfig};
use serde::{Deserialize, Serialize};

#[derive(Clone,Debug)]
pub struct Context{
	bucket_name:String,
	s3_client: aws_sdk_s3::Client,
	redis_client: redis::aio::ConnectionManager,
}
async fn shutdown_signal() {
	use tokio::signal;
	use futures::{future::FutureExt,pin_mut};
	let ctrl_c = async {
		signal::ctrl_c()
			.await
			.expect("failed to install Ctrl+C handler");
	}.fuse();

	#[cfg(unix)]
	let terminate = async {
		signal::unix::signal(signal::unix::SignalKind::terminate())
			.expect("failed to install signal handler")
			.recv()
			.await;
	}.fuse();
	#[cfg(not(unix))]
	let terminate = std::future::pending::<()>().fuse();
	pin_mut!(ctrl_c, terminate);
	futures::select!{
		_ = ctrl_c => {},
		_ = terminate => {},
	}
}
fn main() {
	let rt=tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
	rt.block_on(async{
		let http_addr:SocketAddr={
			std::env::var("BIND_ADDR").unwrap_or("0.0.0.0:8088".into()).parse()
		}.expect("BIND_ADDR");
		let app = Router::new();
		let _ = std::env::var("BASE_URL").expect("BASE_URL");
		let bucket_name = std::env::var("S3_BUCKET").expect("S3_BUCKET");
		let access_key_id= std::env::var("S3_ACCESS_KEY").expect("S3_ACCESS_KEY");
		let secret_access_key= std::env::var("S3_SECRET_KEY").expect("S3_SECRET_KEY");
		let credentials_provider = aws_sdk_s3::config::Credentials::new(access_key_id, secret_access_key, None, None, "s3");
		let config = aws_sdk_s3::Config::builder()
			.behavior_version_latest()
			.credentials_provider(credentials_provider)
			.region(aws_sdk_s3::config::Region::new(std::env::var("S3_REGION").unwrap_or("ap-northeast-1".into())))
			.force_path_style(std::env::var("S3_PATH_STYLE").unwrap_or("true".into()).as_str()=="true")
			.endpoint_url(std::env::var("S3_URL").expect("S3_URL"))
			.build();
		let s3_client = aws_sdk_s3::Client::from_conf(config);
		let redis_client = redis::Client::open(std::env::var("REDIS_URL").expect("REDIS_URL")).expect("redis");
		let redis_config=ConnectionManagerConfig::new();
		let redis_client=redis::aio::ConnectionManager::new_with_config(redis_client,redis_config).await.expect("redis");
		let state=Arc::new(Context{
			bucket_name,
			s3_client,
			redis_client,
		});
		reindex(state.clone()).await;
		let app=route(app).with_state(state);
		let listener: tokio::net::TcpListener = tokio::net::TcpListener::bind(&http_addr).await.unwrap();
		println!("server loaded");
		let server=axum::serve(listener,app.into_make_service_with_connect_info::<SocketAddr>());
		let server=server.with_graceful_shutdown(shutdown_signal());
		server.await.unwrap();
	});
}
async fn reindex(state:Arc<Context>){
	let mut redis=state.redis_client.clone();
	let is_init:bool=redis.get("init_index").await.expect("redis init");
	if is_init{
		return;
	}
	let mut response = state.s3_client
		.list_objects_v2()
		.bucket(state.bucket_name.to_owned())
		.max_keys(20)
		.into_paginator()
		.send();
	while let Some(result) = response.next().await {
		match result {
			Ok(output) => {
				for object in output.contents(){
					if !reindex_by_object(object, state.clone()).await.is_some(){
						println!("skip index - {}", object.key().unwrap_or("Unknown"));
					}
				}
			}
			Err(err) => {
				eprintln!("{err:?}")
			}
		}
	}
	let _:()=redis.set("init_index",true).await.expect("redis init");
}
async fn reindex_by_object(object:&Object,state:Arc<Context>)->Option<()>{
	let mut redis=state.redis_client.clone();
	let meta=state.s3_client.get_object_tagging();
	let meta=meta.bucket(&state.bucket_name).key(object.key()?.to_string()).send().await.ok()?;
	let mut tags=HashMap::new();
	for tag in meta.tag_set{
		tags.insert(tag.key, tag.value);
	}
	let key=tags.get("key")?.to_string();
	let version=tags.get("version")?.to_string();
	let date=object.last_modified()?.to_millis().ok()?;
	let now=chrono::Utc::now().timestamp_millis();
	let entry=CacheEntry{
		id:object.key()?.to_string(),
		created_at:date,
		used_at:now,//最終利用時刻は不明だが、現在時刻に更新する
		key,
		version,
		size:object.size?,
	};
	let res: Result<(), redis::RedisError>=redis.set(format!("{}/{}/complete",entry.key,entry.version),&entry.id).await;
	if let Err(e)=res{
		eprintln!("{}:{} {:?}",file!(),line!(),e);
	}
	let res: Result<(), redis::RedisError>=redis.set(format!("{}",entry.id),serde_json::to_string(&entry).unwrap()).await;
	if let Err(e)=res{
		eprintln!("{}:{} {:?}",file!(),line!(),e);
	}
	println!("reindex {}/{}/{}",entry.key,entry.version,entry.id);
	Some(())
}
pub fn route(app: Router<Arc<Context>>)->Router<Arc<Context>>{
	let app=app.route("/_apis/artifactcache/cache",axum::routing::get(find));
	let app=app.route("/_apis/artifactcache/caches",axum::routing::post(reserve));
	let app=app.route("/_apis/artifactcache/caches/:id",axum::routing::patch(upload));
	let app=app.route("/_apis/artifactcache/caches/:id",axum::routing::post(commit));
	let app=app.route("/_apis/artifactcache/artifacts/:id",axum::routing::get(get));
	let app=app.route("/_apis/artifactcache/clean",axum::routing::post(clean));
	app
}
#[derive(Serialize,Deserialize,Debug)]
struct CacheEntry{
	id:String,
	created_at:i64,
	used_at:i64,
	key:String,
	version:String,
	size:i64,
}
#[derive(Serialize,Deserialize,Debug)]
struct FindQuery{
	keys:String,
	version:String,
}
struct DataStream(aws_sdk_s3::primitives::ByteStream);
impl futures::stream::Stream for DataStream{
	type Item=Result<axum::body::Bytes, aws_smithy_types::byte_stream::error::Error>;

	fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
		let mut r=self.as_mut();
		let ptr=Pin::new(&mut r.0);
		ptr.poll_next(cx)
	}
}
// GET /_apis/artifactcache/cache
async fn find(State(state): State<Arc<Context>>,axum::extract::Query(params): axum::extract::Query<FindQuery>,)->axum::response::Response{
	println!("find {}/{}",params.keys,params.version);
	let keys=params.keys.split(",");
	let mut redis=state.redis_client.clone();
	for key in keys{
		let key=key.to_lowercase();
		let v: Result<String, redis::RedisError>=redis.get(format!("{}/{}/complete",key,params.version)).await;
		//FIXME 部分一致リクエストは非対応
		match v{
			Ok(id)=>{
				println!("cache hit {}",id);
				let object=state.s3_client.get_object_tagging().bucket(&state.bucket_name).key(&id).send().await;
				if let Err(e)=object{
					eprintln!("no object {:?}",e);
					let _: Result<(), redis::RedisError>=redis.del(format!("{}/{}/complete",key,params.version)).await;
					let _: Result<(), redis::RedisError>=redis.del(format!("{}",id)).await;
					return response_json(StatusCode::NO_CONTENT,None);
				}
				let url = std::env::var("BASE_URL").expect("BASE_URL");
				let json=serde_json::json!({
					"result": "hit",
					"archiveLocation":format!("{}/_apis/artifactcache/artifacts/{}",url,id),
					"cacheKey": key,
				}).to_string();
				return response_json(StatusCode::OK,Some(json));
			},
			_=>{}
		}
	}
	return response_json(StatusCode::NO_CONTENT,None);
}
#[derive(Serialize,Deserialize,Debug)]
struct ReserveBody{
	#[serde(rename="key")]
	key:String,
	#[serde(rename="version")]
	version:String,
	#[serde(rename="cacheSize")]
	size:i64,
}
// POST /_apis/artifactcache/caches
async fn reserve(State(state): State<Arc<Context>>,axum::extract::Json(body): axum::extract::Json<ReserveBody>)->axum::response::Response{
	let key=body.key.to_lowercase();
	let mut redis=state.redis_client.clone();
	let id=uuid::Uuid::new_v4();
	let now=chrono::Utc::now().timestamp_millis();
	let entry=serde_json::to_string(&CacheEntry{
		id:id.to_string(),
		created_at:now,
		used_at:now,
		key,
		version:body.version,
		size:body.size,
	}).unwrap();
	println!("reserve {:?}",entry);
	let _res: Result<(), redis::RedisError>=redis.set(id.to_string(),entry).await;
	let resp=serde_json::to_string(&serde_json::json!({
		"cacheId":id.to_string(),
	})).unwrap();
	return response_json(StatusCode::OK,Some(resp));
}
// PATCH /_apis/artifactcache/caches/:id
async fn upload(State(state): State<Arc<Context>>,Path(id):Path<String>,request: axum::extract::Request,)->axum::response::Response{
	println!("upload");
	let mut redis=state.redis_client.clone();
	let res: Result<String, redis::RedisError>=redis.get(id).await;
	let json=match res{
		Ok(v)=>v,
		Err(e)=>{
			eprintln!("{}:{} {:?}",file!(),line!(),e);
			return response_json(StatusCode::NOT_FOUND,None);
		}
	};
	let entry:CacheEntry=match serde_json::from_str(&json){
		Ok(v)=>v,
		Err(e)=>{
			eprintln!("{}:{} {:?}",file!(),line!(),e);
			return response_json(StatusCode::NOT_FOUND,None);
		}
	};
	let v: Result<String, redis::RedisError>=redis.get(format!("{}/{}/complete",entry.key,entry.version)).await;
	if v.is_ok(){
		return  response_json(StatusCode::BAD_REQUEST,None);
	}
	let start={
		let headers=request.headers();
		let v=headers.get("Content-Range").map(|v|v.as_bytes()).map(|v|headers::HeaderValue::from_bytes(v).map(|v|ContentRange::decode(&mut vec![v].iter())));
		match v{
			Some(Ok(Ok(v)))=>v.bytes_range().unwrap_or_default().0,
			_=>0,
		}
	};
	let mut bytes=Vec::with_capacity(8*1024*1024);
	let mut body=request.into_body().into_data_stream();
	while let Some(Ok(next)) = body.next().await {
		bytes.extend_from_slice(&next);
	}
	println!("part {}〜{}",start,start as usize+bytes.len());
	let put=state.s3_client.put_object();
	let put=put.bucket(&state.bucket_name);
	let put=put.key(format!("{}/{}",entry.id,start));
	let put=put.body(ByteStream::new(SdkBody::from(bytes)));
	let res=put.send().await;
	match res{
		Ok(s)=>{
			println!("{:?}",s.size);
			let res: Result<(), redis::RedisError>=redis.lpush(format!("{}/progress",entry.id), start).await;
			if let Err(e)=res{
				eprintln!("{}:{} {:?}",file!(),line!(),e);
				return response_json(StatusCode::INTERNAL_SERVER_ERROR,None);
			}
			return response_json(StatusCode::OK,None);
		},
		Err(e)=>{
			eprintln!("{}:{} {:?}",file!(),line!(),e);
			return response_json(StatusCode::INTERNAL_SERVER_ERROR,None);
		}
	}
}
// POST /_apis/artifactcache/caches/:id
async fn commit(State(state): State<Arc<Context>>,Path(id):Path<String>)->axum::response::Response{
	println!("commit {}",id);
	let mut redis=state.redis_client.clone();
	let part_all: Result<Vec<i64>, redis::RedisError>=redis.lrange(format!("{}/progress",id), 0,-1).await;
	let mut part_all=match part_all{
		Ok(v)=>v,
		Err(e)=>{
			eprintln!("{}:{} {:?}",file!(),line!(),e);
			return response_json(StatusCode::BAD_REQUEST,None);
		}
	};
	part_all.sort();
	let res: Result<(), redis::RedisError>=redis.lrem(format!("{}/progress",id), 0, "*").await;
	if let Err(e)=res{
		eprintln!("{}:{} {:?}",file!(),line!(),e);
	}
	tokio::runtime::Handle::current().spawn(merge(state, part_all, id));
	return response_json(StatusCode::NOT_IMPLEMENTED,None);
}
// GET /_apis/artifactcache/artifacts/:id
async fn get(State(state): State<Arc<Context>>,Path(id):Path<String>)->axum::response::Response{
	println!("get {}",id);
	let object=state.s3_client.get_object().bucket(&state.bucket_name).key(id).send().await;
	match object{
		Err(e)=>eprintln!("{}:{} {:?}",file!(),line!(),e),
		Ok(out)=>{
			return (StatusCode::OK,axum::body::Body::from_stream(DataStream(out.body))).into_response();
		}
	}
	return response_json(StatusCode::NOT_FOUND,None);
}
// POST /_apis/artifactcache/clean
async fn clean(State(_state): State<Arc<Context>>,_request: axum::extract::Request,)->axum::response::Response{
	return response_json(StatusCode::OK,None);
}
async fn merge(state:Arc<Context>,part_all:Vec<i64>,id:String){
	let mut buffer:Vec<u8>=Vec::with_capacity(64*1024*1024);//64MiB
	let mut multipart_id:Option<String>=None;
	let mut upload_parts: Vec<aws_sdk_s3::types::CompletedPart> = Vec::new();
	let mut part_number=0;
	let mut total_size=0;
	let mut redis=state.redis_client.clone();
	let entry: Option<CacheEntry>=match {
		let res: Result<String, redis::RedisError>=redis.get(&id).await;
		res
	}{
		Ok(v)=>{
			serde_json::from_str(&v).ok()
		},
		Err(e)=>{
			eprintln!("{}:{} {:?}",file!(),line!(),e);
			None
		}
	};
	for start in part_all{
		let get=state.s3_client.get_object();
		let get=get.bucket(&state.bucket_name);
		let get=get.key(format!("{}/{}",&id,start));
		let part=get.send().await;
		match part{
			Err(e)=>{
				eprintln!("{}:{} {:?}",file!(),line!(),e);
				upload_parts.clear();
				break;
			},
			Ok(mut v)=>{
				while let Some(Ok(next)) = v.body.next().await {
					buffer.extend_from_slice(&next);
				}
			}
		}
		if buffer.len()>32*1024*1024{
			if multipart_id.is_none(){
				let m=state.s3_client.create_multipart_upload();
				let m=match &entry{
					Some(entry)=>{
						println!("s3 metadata {}/{}",entry.key,entry.version);
						m.tagging(format!("key={}&version={}",&entry.key,&entry.version))
					},
					None=>m,
				};
				let res=m.bucket(&state.bucket_name).key(&id).send().await;
				match res.map(|res|res.upload_id){
					Ok(Some(upload_id))=>{
						multipart_id=Some(upload_id);
					},
					e=>{
						eprintln!("{}:{} {:?}",file!(),line!(),e);
						break;
					}
				}
			}
			if let Some(multipart_id)=&multipart_id{
				part_number+=1;
				let p=state.s3_client.upload_part().bucket(&state.bucket_name).key(&id).upload_id(multipart_id).part_number(part_number);
				let res=p.body(ByteStream::new(SdkBody::from(buffer.clone()))).send().await;
				total_size+=buffer.len() as i64;
				buffer.clear();
				match res{
					Ok(v)=>{
						upload_parts.push(
							CompletedPart::builder()
								.e_tag(v.e_tag.unwrap_or_default())
								.part_number(part_number)
								.build(),
						);
					},
					Err(e)=>{
						eprintln!("{}:{} {:?}",file!(),line!(),e);
						upload_parts.clear();
						break;
					}
				}
			}
		}
	}
	if let Some(multipart_id)=&multipart_id{
		if !buffer.is_empty(){
			total_size+=buffer.len() as i64;
			part_number+=1;
			let p=state.s3_client.upload_part().bucket(&state.bucket_name).key(&id).upload_id(multipart_id).part_number(part_number);
			let res=p.body(ByteStream::new(SdkBody::from(buffer.clone()))).send().await;
			buffer.clear();
			match res{
				Ok(v)=>{
					upload_parts.push(
						CompletedPart::builder()
							.e_tag(v.e_tag.unwrap_or_default())
							.part_number(part_number)
							.build(),
					);
				},
				Err(e)=>{
					eprintln!("{}:{} {:?}",file!(),line!(),e);
					upload_parts.clear();
				}
			}
		}
		if !upload_parts.is_empty(){
			let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
				.set_parts(Some(upload_parts))
				.build();
			let res = state.s3_client
				.complete_multipart_upload()
				.bucket(&state.bucket_name)
				.key(&id)
				.multipart_upload(completed_multipart_upload)
				.upload_id(multipart_id)
				.send()
				.await;
			if let Err(e)=res{
				eprintln!("{}:{} {:?}",file!(),line!(),e);
			}else{
				if let Some(mut entry)=entry{
					let res: Result<(), redis::RedisError>=redis.set(format!("{}/{}/complete",entry.key,entry.version),&id).await;
					if let Err(e)=res{
						eprintln!("{}:{} {:?}",file!(),line!(),e);
					}
					entry.size=total_size;
					let res: Result<(), redis::RedisError>=redis.set(format!("{}",entry.id),serde_json::to_string(&entry).unwrap()).await;
					if let Err(e)=res{
						eprintln!("{}:{} {:?}",file!(),line!(),e);
					}
				}
			}
		}else{
			let abort=state.s3_client.abort_multipart_upload();
			let res=abort.bucket(&state.bucket_name).key(multipart_id).send().await;
			if let Err(e)=res{
				eprintln!("{}:{} {:?}",file!(),line!(),e);
			}
		}
	}else{
		if let Some(mut entry)=entry{
			println!("s3 metadata {}/{}",entry.key,entry.version);
			let body=ByteStream::new(SdkBody::from(buffer.clone()));
			let builder=state.s3_client.put_object();
			let builder=builder.bucket(&state.bucket_name);
			let tag=format!("key={}&version={}",&entry.key,&entry.version);
			let builder=builder.key(&id).tagging(tag);
			let res=builder.body(body).send().await;
			if let Err(e)=res{
				eprintln!("{}:{} {:?}",file!(),line!(),e);
			}else{
				let res: Result<(), redis::RedisError>=redis.set(format!("{}/{}/complete",entry.key,entry.version),&id).await;
				if let Err(e)=res{
					eprintln!("{}:{} {:?}",file!(),line!(),e);
				}
				entry.size=buffer.len() as i64;
				let res: Result<(), redis::RedisError>=redis.set(format!("{}",entry.id),serde_json::to_string(&entry).unwrap()).await;
				if let Err(e)=res{
					eprintln!("{}:{} {:?}",file!(),line!(),e);
				}
			}
		}
	}
}
fn response_json(status:StatusCode,body:Option<String>)->axum::response::Response{
	let mut resp=match body{
		Some(b)=>(status,b).into_response(),
		None=>(status,"{}").into_response(),
	};
	let header=resp.headers_mut();
	header.append("Content-Type", "application/json; charset=utf-8".parse().unwrap());
	resp
}
