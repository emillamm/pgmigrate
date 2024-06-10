FROM golang:1.22
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN ls -R
RUN CGO_ENABLED=0 GOOS=linux go build -o /pgmigrate ./cmd/pgmigrate
CMD ["/pgmigrate"]

