FROM scratch
ENV PATH=/bin

COPY qlrx /bin/

WORKDIR /

ENTRYPOINT ["/bin/qlrx"]