FROM rust:buster
RUN apt-get update && \
	apt-get install --yes libsodium-dev awscli python-pip wget rclone openssl socat && \
	    rm -rf /var/lib/apt/lists/*
RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc -O /usr/local/bin/mc && chmod +x /usr/local/bin/mc
RUN rustup component add rustfmt clippy
RUN pip install s3cmd
