FROM golang:1.13.13-alpine3.12 as BUILDER
WORKDIR /workspace
COPY . .
RUN make cmd/kured/kured

FROM alpine:3.12
RUN apk add --no-cache ca-certificates tzdata
# NB: you may need to update RBAC permissions when upgrading kubectl - see kured-rbac.yaml for details
ADD https://storage.googleapis.com/kubernetes-release/release/v1.15.4/bin/linux/amd64/kubectl /usr/bin/kubectl
RUN chmod 0755 /usr/bin/kubectl
COPY --from=builder /workspace/cmd/kured/kured .
ENTRYPOINT ["/usr/bin/kured"]
