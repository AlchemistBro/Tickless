package outbox

import "go.opentelemetry.io/otel/attribute"

func attrOutbox(name string) attribute.KeyValue { return attribute.String("outbox", name) }
