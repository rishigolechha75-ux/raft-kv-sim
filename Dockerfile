FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir . && useradd --create-home raft && mkdir -p /data && chown raft /data
USER raft

EXPOSE 7000
VOLUME ["/data"]
ENTRYPOINT ["raftkv"]
CMD ["serve", "--id", "n0", "--cluster", "n0=0.0.0.0:7000", "--data-dir", "/data"]
