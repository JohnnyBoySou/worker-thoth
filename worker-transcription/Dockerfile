# --- build stage ---
FROM golang:1.26 AS build
WORKDIR /src

# Cache module downloads.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# Static binary so it runs on a scratch/distroless base.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/server

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/worker /app/worker

ENV PORT=8770
EXPOSE 8770

USER nonroot:nonroot
ENTRYPOINT ["/app/worker"]
