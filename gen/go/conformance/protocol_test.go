package conformance_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
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
	if got, want := protocolv2.ExecutionService_StreamRunOutput_FullMethodName,
		"/agnt5.protocol.v2.ExecutionService/StreamRunOutput"; got != want {
		t.Fatalf("StreamRunOutput method = %q, want %q", got, want)
	}
	if got, want := protocolv2.PayloadService_PutPayload_FullMethodName,
		"/agnt5.protocol.v2.PayloadService/PutPayload"; got != want {
		t.Fatalf("PutPayload method = %q, want %q", got, want)
	}
	if got, want := protocolv2.EventService_PublishEvent_FullMethodName,
		"/agnt5.protocol.v2.EventService/PublishEvent"; got != want {
		t.Fatalf("PublishEvent method = %q, want %q", got, want)
	}
}

func TestEndpointSignatureFixture(t *testing.T) {
	var fixture struct {
		SecretUTF8        string `json:"secret_utf8"`
		ExecutionID       string `json:"execution_id"`
		RawBody           string `json:"raw_body"`
		SigningInput      string `json:"signing_input"`
		ExpectedSignature string `json:"expected_signature"`
	}
	readJSON(t, fixturePath("endpoint-signature-v1.json"), &fixture)

	var request protocolv2.InvokeEndpointRequest
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal([]byte(fixture.RawBody), &request); err != nil {
		t.Fatalf("unmarshal signed endpoint request: %v", err)
	}
	if request.ExecutionId != fixture.ExecutionID {
		t.Fatalf("execution_id = %q, want %q", request.ExecutionId, fixture.ExecutionID)
	}

	mac := hmac.New(sha256.New, []byte(fixture.SecretUTF8))
	_, _ = mac.Write([]byte(fixture.SigningInput))
	got := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if got != fixture.ExpectedSignature {
		t.Fatalf("signature = %q, want %q", got, fixture.ExpectedSignature)
	}
}

func TestComponentAndPayloadProtoJSONFixtures(t *testing.T) {
	componentJSON, err := os.ReadFile(fixturePath("component-descriptor-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var descriptor protocolv2.ComponentDescriptor
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(componentJSON, &descriptor); err != nil {
		t.Fatalf("unmarshal component descriptor: %v", err)
	}
	if descriptor.Version == "" || len(descriptor.Methods) != 1 || descriptor.Methods[0].Name != "add_item" {
		t.Fatalf(
			"unexpected component descriptor name=%q version=%q methods=%d",
			descriptor.Name,
			descriptor.Version,
			len(descriptor.Methods),
		)
	}

	var payloadFixture struct {
		PutFrames []json.RawMessage `json:"put_frames"`
	}
	readJSON(t, fixturePath("payload-transfer-v1.json"), &payloadFixture)
	if len(payloadFixture.PutFrames) != 2 {
		t.Fatalf("put frame count = %d, want 2", len(payloadFixture.PutFrames))
	}
	var metadataFrame, chunkFrame protocolv2.PutPayloadRequest
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(payloadFixture.PutFrames[0], &metadataFrame); err != nil {
		t.Fatalf("unmarshal payload metadata: %v", err)
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(payloadFixture.PutFrames[1], &chunkFrame); err != nil {
		t.Fatalf("unmarshal payload chunk: %v", err)
	}
	if metadataFrame.GetMetadata().GetRequestId() != "payload-request-01" {
		t.Fatalf("unexpected metadata frame: %+v", metadataFrame.GetMetadata())
	}
	if got := string(chunkFrame.GetChunk().GetData()); got != `{"name":"Ada"}` {
		t.Fatalf("payload chunk = %q", got)
	}
}

func TestErrorMappingCoversEveryProtocolCode(t *testing.T) {
	var mapping struct {
		Errors []struct {
			ProtoCode        string `json:"proto_code"`
			GRPCCode         string `json:"grpc_code"`
			HTTPStatus       int    `json:"http_status"`
			DefaultRetryable bool   `json:"default_retryable"`
		} `json:"errors"`
	}
	readJSON(t, specPath("error-mapping.json"), &mapping)

	seen := make(map[string]bool, len(mapping.Errors))
	for _, entry := range mapping.Errors {
		if seen[entry.ProtoCode] {
			t.Fatalf("duplicate protocol error mapping for %s", entry.ProtoCode)
		}
		seen[entry.ProtoCode] = true
		if entry.GRPCCode == "" || entry.HTTPStatus < 400 || entry.HTTPStatus > 599 {
			t.Fatalf("invalid mapping for %s: %+v", entry.ProtoCode, entry)
		}
	}
	for _, name := range protocolv2.ProtocolErrorCode_name {
		if !seen[name] {
			t.Errorf("missing protocol error mapping for %s", name)
		}
	}
	if len(seen) != len(protocolv2.ProtocolErrorCode_name) {
		t.Fatalf("mapped %d error codes, generated enum has %d", len(seen), len(protocolv2.ProtocolErrorCode_name))
	}
}

func TestCapabilityRegistryIsUniqueAndSorted(t *testing.T) {
	var registry struct {
		Capabilities []struct {
			Name    string `json:"name"`
			Version uint32 `json:"version"`
		} `json:"capabilities"`
	}
	readJSON(t, specPath("capabilities.json"), &registry)

	seen := make(map[string]bool, len(registry.Capabilities))
	previous := ""
	for _, capability := range registry.Capabilities {
		if capability.Name == "" || capability.Version == 0 {
			t.Fatalf("invalid capability: %+v", capability)
		}
		if seen[capability.Name] {
			t.Fatalf("duplicate capability %q", capability.Name)
		}
		if previous != "" && capability.Name < previous {
			t.Fatalf("capability registry is not sorted: %q before %q", previous, capability.Name)
		}
		seen[capability.Name] = true
		previous = capability.Name
	}
}

func fixturePath(name string) string {
	return filepath.Join("..", "..", "..", "tests", "conformance", "v2", "fixtures", name)
}

func specPath(name string) string {
	return filepath.Join("..", "..", "..", "proto", "agnt5", "protocol", "v2", "spec", name)
}

func readJSON(t *testing.T, path string, destination any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := json.Unmarshal(data, destination); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}
