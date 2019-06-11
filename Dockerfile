FROM alpine:3.9.4
ENV PATH=/bin

COPY qlrx /bin/

WORKDIR /

ENTRYPOINT ["/bin/qlrx"]