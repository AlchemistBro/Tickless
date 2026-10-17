// Package otelx wires OpenTelemetry traces and metrics (OTLP/gRPC to the
// collector) and a slog handler that stamps trace_id and span_id on every log
// line, so logs, traces and metrics of every service correlate.
package otelx

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Setup installs global tracer/meter providers and the W3C propagator and
// returns a shutdown function. The OTLP endpoint comes from the standard
// OTEL_EXPORTER_OTLP_ENDPOINT variable (default http://localhost:4317). When
// OTEL_SDK_DISABLED=true nothing is exported (unit tests).
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	InitLogger(service)
	if os.Getenv("OTEL_SDK_DISABLED") == "true" {
		return func(context.Context) error { return nil }, nil
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(semconv.ServiceName(service)))
	if err != nil {
		return nil, err
	}
	texp, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(texp, sdktrace.WithBatchTimeout(time.Second)), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)

	mexp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(mexp, sdkmetric.WithInterval(5*time.Second))),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	return func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}, nil
}

// Tracer returns the tracer for a component.
func Tracer(name string) trace.Tracer { return otel.Tracer("tickless/" + name) }

// InitLogger installs a JSON slog handler (default logger) that adds
// service, trace_id and span_id attributes.
func InitLogger(service string) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: levelFromEnv()})
	slog.SetDefault(slog.New(&traceHandler{Handler: h}).With(slog.String("service", service)))
}

func levelFromEnv() slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		return slog.LevelInfo
	}
	return l
}

type traceHandler struct{ slog.Handler }

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()), slog.String("span_id", sc.SpanID().String()))
	}
	return h.Handler.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithAttrs(a)}
}

func (h *traceHandler) WithGroup(n string) slog.Handler {
	return &traceHandler{Handler: h.Handler.WithGroup(n)}
}

// Attr is a shorthand for attribute.String.
func Attr(k, v string) attribute.KeyValue { return attribute.String(k, v) }
