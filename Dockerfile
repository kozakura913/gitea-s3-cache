FROM public.ecr.aws/docker/library/rust:latest AS build_app
RUN apt-get update && apt-get install -y clang musl-dev pkg-config git
ENV CARGO_HOME=/var/cache/cargo
RUN rustup target add x86_64-unknown-linux-musl
ENV SYSTEM_DEPS_LINK=static
WORKDIR /app
COPY src ./src
COPY Cargo.toml ./Cargo.toml
RUN --mount=type=cache,target=/var/cache/cargo --mount=type=cache,target=/app/target cargo build --target x86_64-unknown-linux-musl --release && cp -a /app/target/x86_64-unknown-linux-musl/release/gitea-s3-cache /app/

FROM public.ecr.aws/docker/library/alpine:latest
ARG UID="982"
ARG GID="982"
RUN addgroup -g "${GID}" giteacache && adduser -u "${UID}" -G giteacache -D -h /gitea-s3-cache -s /bin/sh proxy
WORKDIR /gitea-s3-cache
USER giteacache
COPY --from=build_app /app/gitea-s3-cache ./gitea-s3-cache
EXPOSE 8088
CMD ["./gitea-s3-cache"]
