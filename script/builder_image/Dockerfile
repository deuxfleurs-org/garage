FROM rust:buster
RUN apt-get update && \
	apt-get install --yes libsodium-dev awscli python-pip && \
	    rm -rf /var/lib/apt/lists/*
RUN rustup component add rustfmt
RUN pip install s3cmd
