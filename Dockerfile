FROM archlinux:latest

RUN mkdir -p /garage/meta
RUN mkdir -p /garage/data
ENV RUST_BACKTRACE=1
ENV RUST_LOG=garage=info

COPY target/release/garage.stripped /garage/garage

CMD /garage/garage server -c /garage/config.toml
