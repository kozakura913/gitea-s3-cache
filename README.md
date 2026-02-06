# gitea-s3-cache
Gitea Actionsの外部キャッシュサーバー

## 使い方
`.devcontainer/docker-compose.yml`をコピーして環境に合わせて調整する。これ自体はファイルを読み書きしない。  
"S3互換API"と"redis互換API"が使える必要がある。  
act_runner側のcache:external_serverに`gitea-s3-cache`のホスト、ポートを指定する。DNS使用可能。  
redisのデータを全削除するとs3側のデータから再インデックスする。不整合を起こしたりs3側のデータを整理した時に使える。  
BASE_URL環境変数にはact_runnerからアクセスする際に使うURLを指定する  
現在のDockerfileはx86_64-linuxしか対応していない  

## 未実装の機能
`/_apis/artifactcache/clean`エンドポイントは何も実行しない  
キャッシュの最終利用日時が更新されない  
キャッシュの自動削除  
正規表現による取得  
S3からのキャッシュ本体の直接取得  
