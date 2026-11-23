// otlp-interop sends logs to StrataDB with the official OpenTelemetry Go SDK
// OTLP/HTTP log exporter (protobuf + gzip) to verify real-client interoperability.
//
//	go run . -endpoint 127.0.0.1:7070 -token sdb_... -n 50
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func main() {
	endpoint := flag.String("endpoint", "127.0.0.1:7070", "host:port")
	token := flag.String("token", "", "StrataDB API token")
	n := flag.Int("n", 50, "number of log records")
	flag.Parse()
	ctx := context.Background()
	exp, err := otlploghttp.New(ctx,
		otlploghttp.WithEndpoint(*endpoint),
		otlploghttp.WithInsecure(),
		otlploghttp.WithURLPath("/v1/logs"),
		otlploghttp.WithCompression(otlploghttp.GzipCompression),
		otlploghttp.WithHeaders(map[string]string{"Authorization": "Bearer " + *token}),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "exporter:", err)
		os.Exit(1)
	}
	res := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName("interop-svc"))
	provider := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)))
	logger := provider.Logger("interop.scope")
	for i := 0; i < *n; i++ {
		var r otellog.Record
		r.SetTimestamp(time.Now())
		r.SetSeverity(otellog.SeverityError)
		r.SetSeverityText("ERROR")
		r.SetBody(attribute.StringValue(fmt.Sprintf("interop message %d", i)))
		r.AddAttributes(attribute.Int("i", i), attribute.String("request_id", fmt.Sprintf("req-%d", i)), attribute.Bool("ok", false))
		r.SetEventName("interop.event")
		logger.Emit(ctx, r)
	}
	if err := provider.Shutdown(ctx); err != nil { // flushes the batch processor
		fmt.Fprintln(os.Stderr, "shutdown/export:", err)
		os.Exit(1)
	}
	fmt.Println("exported", *n)
}
