FROM golang:1.22 as builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN ls -R
RUN CGO_ENABLED=0 GOOS=linux go build -o pgmigrate ./cmd/pgmigrate

FROM gcr.io/distroless/base-debian10
COPY --from=builder /app/pgmigrate /
CMD ["/pgmigrate"]

