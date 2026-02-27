FROM scratch

ENV RUST_BACKTRACE=1
ENV RUST_LOG=garage=info

COPY result/bin/garage /

ENTRYPOINT ["/garage"]
CMD ["server"]
