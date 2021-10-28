FROM scratch

ENV RUST_BACKTRACE=1
ENV RUST_LOG=garage=info

COPY result/bin/garage /
CMD [ "/garage", "server"]
