FROM rust:1.93 AS chef

# Stop if a command fails
RUN set -eux

# Only fetch crates.io index for used crates
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# cargo-chef will be cached from the second build onwards
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json

# Build application
COPY . .
RUN cargo build --release --bin server

FROM debian:bookworm-slim AS runtime
WORKDIR /app

# Install Jepsen's "toolbox"
RUN apt-get update && apt-get install -y \
    openssh-server \
    sudo \
    iptables \
    iproute2 \
    iputils-ping \
    util-linux \
    && rm -rf /var/lib/apt/lists/*

# Setup SSH: Allow root login with password 'root', read /etc/environment
RUN mkdir /var/run/sshd && \
    echo 'root:root' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    echo 'PermitUserEnvironment yes' >> /etc/ssh/sshd_config

# Ensure sudo doesn't prompt for password (required by Jepsen scripts)
RUN echo "root ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

# Entrypoint: export docker-compose env vars to /etc/environment so SSH
# sessions can read them, then start sshd.
RUN cat > /entrypoint.sh << 'EOF'
#!/bin/bash
printenv | grep -v "HOME\|PWD\|PATH\|SHLVL\|_=" >> /etc/environment
exec /usr/sbin/sshd -D
EOF
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh


# Copy your Rust binary
COPY --from=builder /app/target/release/server /usr/local/bin/omnipaxos-server

EXPOSE 22 8000 9000
ENTRYPOINT ["/entrypoint.sh"]
