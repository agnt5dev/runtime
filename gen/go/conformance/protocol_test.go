package conformance_test

import (
	"strings"
	"testing"
	"time"

	protocolv2 "github.com/agnt5dev/runtime/gen/go/agnt5/protocol/v2"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestComponentDescriptorUsesPortableV2Shape(t *testing.T) {
	descriptor := &protocolv2.ComponentDescriptor{
		Type:    protocolv2.ComponentType_COMPONENT_TYPE_WORKFLOW,
		Name:    "order_workflow",
		Version: "v1",
		ExecutionDefaults: &protocolv2.ExecutionDefaults{
			RetryPolicy: &protocolv2.RetryPolicy{
				MaximumAttempts: 3,
				BackoffStrategy: protocolv2.RetryBackoffStrategy_RETRY_BACKOFF_STRATEGY_EXPONENTIAL,
			},
			ExecutionTimeout: durationpb.New(30 * time.Second),
		},
		Triggers: []*protocolv2.TriggerDescriptor{
			{
				TriggerId: "order-created",
				Kind: &protocolv2.TriggerDescriptor_Event{
					Event: &protocolv2.EventTrigger{EventName: "order.created"},
				},
			},
		},
	}

	encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(descriptor)
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	json := string(encoded)
	for _, field := range []string{
		`"execution_defaults"`,
		`"maximum_attempts"`,
		`"execution_timeout"`,
		`"trigger_id"`,
		`"event_name"`,
	} {
		if !strings.Contains(json, field) {
			t.Fatalf("generated proto JSON missing %s: %s", field, json)
		}
	}
}

func TestWorkerServiceMethodNamesAreStable(t *testing.T) {
	if got, want := protocolv2.WorkerService_RegisterWorker_FullMethodName,
		"/agnt5.protocol.v2.WorkerService/RegisterWorker"; got != want {
		t.Fatalf("RegisterWorker method = %q, want %q", got, want)
	}
	if got, want := protocolv2.WorkerService_CommitRunOutcome_FullMethodName,
		"/agnt5.protocol.v2.WorkerService/CommitRunOutcome"; got != want {
		t.Fatalf("CommitRunOutcome method = %q, want %q", got, want)
	}
}
