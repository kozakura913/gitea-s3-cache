FROM rust:latest
RUN apt-get update && apt-get install -y clang pkg-config
ENV RUSTFLAGS=""
ENV CC=clang
ARG UID="1000"
ARG GID="1000"
RUN groupadd -g "${GID}" giteacache && useradd -l -u "${UID}" -g "${GID}" -m -d /giteacache giteacache -s /bin/bash
USER giteacache
