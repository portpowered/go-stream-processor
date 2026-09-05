package engine

// ControlType identifies the kind of out-of-band control message.
type ControlType int

const (
	// ControlTypeCheckpointRequest asks a node to initiate a checkpoint.
	ControlTypeCheckpointRequest ControlType = iota
	// ControlTypePause asks a node to pause processing.
	ControlTypePause
	// ControlTypeResume asks a paused node to resume processing.
	ControlTypeResume
	// ControlTypeTerminate asks a node to exit immediately.
	ControlTypeTerminate
)

// ControlMessage is an out-of-band message sent to nodes via a dedicated
// control channel. Unlike SignalMessages which flow in-band with data,
// ControlMessages are delivered directly to nodes and handled outside the
// normal data processing path.
type ControlMessage struct {
	// Type identifies what kind of control action is requested.
	Type ControlType
	// Checkpoint holds details for checkpoint requests. Non-nil only when
	// Type == ControlTypeCheckpointRequest.
	Checkpoint *CheckpointRequest
}

// CheckpointRequest contains the details for initiating a new checkpoint.
type CheckpointRequest struct {
	// Epoch is the checkpoint epoch number to create.
	Epoch uint64
	// MinEpoch is the minimum epoch that must be retained.
	MinEpoch uint64
	// ThenStop indicates the pipeline should stop after this checkpoint.
	ThenStop bool
}

// NewCheckpointRequestControl creates a ControlMessage requesting a checkpoint.
func NewCheckpointRequestControl(epoch, minEpoch uint64, thenStop bool) ControlMessage {
	return ControlMessage{
		Type: ControlTypeCheckpointRequest,
		Checkpoint: &CheckpointRequest{
			Epoch:    epoch,
			MinEpoch: minEpoch,
			ThenStop: thenStop,
		},
	}
}

// NewPauseControl creates a ControlMessage requesting a node to pause.
func NewPauseControl() ControlMessage {
	return ControlMessage{Type: ControlTypePause}
}

// NewResumeControl creates a ControlMessage requesting a paused node to resume.
func NewResumeControl() ControlMessage {
	return ControlMessage{Type: ControlTypeResume}
}

// NewTerminateControl creates a ControlMessage requesting immediate termination.
func NewTerminateControl() ControlMessage {
	return ControlMessage{Type: ControlTypeTerminate}
}
