FROM ubuntu:latest

RUN apt-get update && \
    apt-get -y --no-install-recommends install ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    update-ca-certificates

EXPOSE 8185

COPY bin/dwworker-linux-amd64 /bin/dwworker

ENTRYPOINT ["/bin/dwworker"]
