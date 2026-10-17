package wshub

import "go.opentelemetry.io/otel/attribute"

func attrHub(name string) attribute.KeyValue { return attribute.String("hub", name) }
