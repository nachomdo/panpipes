FROM golang:latest

RUN cd /tmp && git clone https://github.com/edenhill/librdkafka && cd librdkafka && ./configure && make && make install
COPY . /go/src/github.com/nachomdo/panpipes
RUN apt-get update && apt-get install -y librdkafka-dev

RUN cd /go/src/github.com/nachomdo/panpipes && go build && cp panpipes /tmp && cd / && rm -rf /go/src

ENTRYPOINT ["/tmp/panpipes"]

