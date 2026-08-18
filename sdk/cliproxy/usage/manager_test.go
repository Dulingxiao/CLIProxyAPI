package usage

import (
	"context"
	"errors"
	"testing"
)

type gatedBuiltinSink struct {
	entered chan struct{}
	release chan struct{}
}

type failingDurableBuiltinSink struct{}

func (*failingDurableBuiltinSink) HandleUsage(context.Context, Record) {}
func (*failingDurableBuiltinSink) HandleUsageDurable(context.Context, Record) error {
	return errors.New("injected durable usage failure")
}

func (s *gatedBuiltinSink) HandleUsage(context.Context, Record) {
	close(s.entered)
	<-s.release
}

func TestPublishWaitsForBuiltinSink(t *testing.T) {
	manager := NewManager(1)
	sink := &gatedBuiltinSink{entered: make(chan struct{}), release: make(chan struct{})}
	manager.SetBuiltinSink(sink)
	returned := make(chan struct{})
	go func() {
		manager.Publish(context.Background(), Record{})
		close(returned)
	}()
	<-sink.entered
	returnedBeforeBuiltin := false
	select {
	case <-returned:
		returnedBeforeBuiltin = true
	default:
	}
	close(sink.release)
	<-returned
	manager.Stop()
	if returnedBeforeBuiltin {
		t.Fatal("Publish returned before builtin sink completed")
	}
}

func TestPublishReturnsDurableBuiltinSinkError(t *testing.T) {
	manager := NewManager(1)
	manager.SetBuiltinSink(&failingDurableBuiltinSink{})
	errPublish := manager.Publish(context.Background(), Record{})
	manager.Stop()
	if errPublish == nil || errPublish.Error() != "injected durable usage failure" {
		t.Fatalf("Publish() error = %v", errPublish)
	}
}

func TestGenerateEnabledDefaultsNilToTrue(t *testing.T) {
	if !GenerateEnabled(nil) {
		t.Fatalf("GenerateEnabled(nil) = false, want true")
	}
}

func TestGenerateEnabledHonorsExplicitFalse(t *testing.T) {
	if GenerateEnabled(GenerateFlag(false)) {
		t.Fatalf("GenerateEnabled(false) = true, want false")
	}
}

func TestGenerateEnabledHonorsExplicitTrue(t *testing.T) {
	if !GenerateEnabled(GenerateFlag(true)) {
		t.Fatalf("GenerateEnabled(true) = false, want true")
	}
}

func TestGenerateFromContextDefaultsMissingToTrue(t *testing.T) {
	if !GenerateFromContext(context.Background()) {
		t.Fatalf("GenerateFromContext(background) = false, want true")
	}
}

func TestGenerateFromContextHonorsExplicitFalse(t *testing.T) {
	ctx := WithGenerate(context.Background(), false)
	if GenerateFromContext(ctx) {
		t.Fatalf("GenerateFromContext(false) = true, want false")
	}
}

func TestRecordOmittedGenerateIsEnabled(t *testing.T) {
	// Existing callers construct Record without setting Generate.
	// Omission must remain distinguishable from explicit false and default to true.
	record := Record{
		Provider: "openai",
		Model:    "gpt-5.4",
	}
	if record.Generate != nil {
		t.Fatalf("Record.Generate = %v, want nil for omitted field", record.Generate)
	}
	if !GenerateEnabled(record.Generate) {
		t.Fatalf("GenerateEnabled(omitted) = false, want true")
	}
}
