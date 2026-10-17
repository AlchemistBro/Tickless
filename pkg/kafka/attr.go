package kafka

import "go.opentelemetry.io/otel/attribute"

func attrString(k, v string) attribute.KeyValue { return attribute.String(k, v) }
