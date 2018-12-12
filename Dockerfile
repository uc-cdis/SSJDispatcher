FROM golang:1.10 as build-deps

RUN go get -tags k8s.io/client-go/kubernetes \
    k8s.io/apimachinery/pkg/apis/meta/v1 \
    k8s.io/api/core/v1 \
    k8s.io/api/batch/v1 \
    k8s.io/client-go/tools/clientcmd \
    k8s.io/client-go/rest

# Build static arborist binary
RUN mkdir -p /go/src/github.com/uc-cdis/ssjdispatcher
WORKDIR /go/src/github.com/uc-cdis/ssjdispatcher
ADD . .
RUN go build -ldflags "-linkmode external -extldflags -static" -o bin/ssjdispatcher

# Set up small scratch image, and copy necessary things over
FROM scratch

COPY --from=build-deps /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build-deps /go/src/github.com/uc-cdis/ssjdispatcher/bin/ssjdispatcher /ssjdispatcher

ENTRYPOINT ["/ssjdispatcher"]