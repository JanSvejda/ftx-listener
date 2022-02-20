FROM rust:1-slim as builder

RUN USER=root cargo new --bin ftx-listener
WORKDIR ./ftx-listener
COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
RUN apt-get update \
        && apt-get install -y pkg-config openssl libssl-dev \
        && rm -rf /var/lib/apt/lists/*
RUN cargo build --release
RUN rm src/*.rs

ADD . ./

RUN rm ./target/release/deps/ftx_listener*
RUN cargo build --release


FROM debian:buster-slim
ARG RUNTIME_DIR=/usr/bin
ENV CONT_USER=appuser


RUN groupadd $CONT_USER \
    && useradd -g $CONT_USER $CONT_USER \
    && mkdir -p ${RUNTIME_DIR}

COPY --from=builder /ftx-listener/target/release/ftx-listener ${RUNTIME_DIR}/ftx-listener

RUN chown -R $CONT_USER:$CONT_USER ${RUNTIME_DIR}

USER $CONT_USER
WORKDIR ${RUNTIME_DIR}

CMD ["./ftx-listener"]