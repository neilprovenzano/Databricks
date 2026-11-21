# Shopping Sessions: native Rust pipeline (no runtime dependencies).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
# Scorers are Cascade Lake or newer: AVX2/BMI2 are available.
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
COPY Cargo.toml Cargo.lock ./
COPY *.rs ./
RUN cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/sessions /app/sessions
USER 1000:1000
CMD ["/app/sessions"]
