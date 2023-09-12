FROM ubuntu:latest

EXPOSE 8185

COPY bin/dwworker-linux-amd64 /bin/dwworker

ENTRYPOINT ["/bin/dwworker"]
