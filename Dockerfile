# Use an intermediate stage to copy only files with the desired extension
FROM alpine AS intermediate
WORKDIR /src
COPY . .

# Find and copy only files with the specified extension to another directory
RUN mkdir /filtered && find . -name '*.go' -exec cp --parents \{\} /filtered/ \;

FROM golang:1.22
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

# Copy the filtered files from the intermediate stage
COPY --from=intermediate /filtered/ .

RUN CGO_ENABLED=0 GOOS=linux go build -o /pgmigrate ./cmd/pgmigrate
CMD ["/pgmigrate"]

