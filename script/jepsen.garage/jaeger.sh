docker stop jaeger
docker rm jaeger

# UI is on localhost:16686
# otel-grpc collector is on localhost:4317
# otel-http collector is on localhost:4318

docker run -d --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 16686:16686 \
  jaegertracing/all-in-one:1.50
