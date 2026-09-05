package engine

import "testing"

func TestNewCheckpointRequestControl(t *testing.T) {
	ctrl := NewCheckpointRequestControl(5, 3, true)

	if ctrl.Type != ControlTypeCheckpointRequest {
		t.Fatalf("expected ControlTypeCheckpointRequest, got %d", ctrl.Type)
	}
	if ctrl.Checkpoint == nil {
		t.Fatal("expected Checkpoint to be non-nil")
	}
	if ctrl.Checkpoint.Epoch != 5 {
		t.Fatalf("expected epoch 5, got %d", ctrl.Checkpoint.Epoch)
	}
	if ctrl.Checkpoint.MinEpoch != 3 {
		t.Fatalf("expected min epoch 3, got %d", ctrl.Checkpoint.MinEpoch)
	}
	if !ctrl.Checkpoint.ThenStop {
		t.Fatal("expected ThenStop to be true")
	}
}

func TestNewPauseControl(t *testing.T) {
	ctrl := NewPauseControl()

	if ctrl.Type != ControlTypePause {
		t.Fatalf("expected ControlTypePause, got %d", ctrl.Type)
	}
	if ctrl.Checkpoint != nil {
		t.Fatal("expected Checkpoint to be nil for pause")
	}
}

func TestNewResumeControl(t *testing.T) {
	ctrl := NewResumeControl()

	if ctrl.Type != ControlTypeResume {
		t.Fatalf("expected ControlTypeResume, got %d", ctrl.Type)
	}
	if ctrl.Checkpoint != nil {
		t.Fatal("expected Checkpoint to be nil for resume")
	}
}

func TestNewTerminateControl(t *testing.T) {
	ctrl := NewTerminateControl()

	if ctrl.Type != ControlTypeTerminate {
		t.Fatalf("expected ControlTypeTerminate, got %d", ctrl.Type)
	}
	if ctrl.Checkpoint != nil {
		t.Fatal("expected Checkpoint to be nil for terminate")
	}
}

func TestControlTypeValues(t *testing.T) {
	// Verify all control types are distinct.
	types := map[ControlType]string{
		ControlTypeCheckpointRequest: "CheckpointRequest",
		ControlTypePause:             "Pause",
		ControlTypeResume:            "Resume",
		ControlTypeTerminate:         "Terminate",
	}

	if len(types) != 4 {
		t.Fatalf("expected 4 distinct control types, got %d", len(types))
	}
}

func TestCheckpointRequestWithoutThenStop(t *testing.T) {
	ctrl := NewCheckpointRequestControl(1, 0, false)

	if ctrl.Checkpoint.ThenStop {
		t.Fatal("expected ThenStop to be false")
	}
}
