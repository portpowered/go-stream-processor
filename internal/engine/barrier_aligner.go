package engine

// indexedMessage wraps a Message with its source input index so the runtime
// can track which input delivered each message. This is required for barrier
// alignment at merge nodes (Chandy-Lamport algorithm).
type indexedMessage struct {
	InputIndex int
	Msg        Message
}

// BarrierAligner implements the Chandy-Lamport barrier alignment protocol
// for nodes with multiple inputs. When a checkpoint barrier arrives on one
// input, data from that input is buffered until all inputs have received
// the barrier for the same epoch. Once aligned, the node snapshots state
// and forwards the barrier downstream.
//
// For nodes with a single input, no alignment is needed and barriers pass
// through immediately.
type BarrierAligner struct {
	numInputs int

	// currentEpoch is the epoch currently being aligned.
	currentEpoch uint64
	// received tracks which input indices have sent their barrier.
	received map[int]bool
	// buffered stores data/signal messages from already-barriered inputs
	// that arrived during alignment.
	buffered []Message
	// aligning indicates an alignment is in progress.
	aligning bool
	// currentBarrier stores the barrier signal being aligned.
	currentBarrier *BarrierSignal
}

// NewBarrierAligner creates a new BarrierAligner for a node with the given
// number of inputs. If numInputs <= 1, the aligner is effectively a no-op
// passthrough (barriers are never held).
func NewBarrierAligner(numInputs int) *BarrierAligner {
	return &BarrierAligner{
		numInputs: numInputs,
		received:  make(map[int]bool),
	}
}

// OnMessage processes an indexed message through the barrier alignment protocol.
//
// Returns:
//   - toProcess: messages that should be processed/dispatched immediately.
//   - alignedBarrier: non-nil if all inputs have aligned on this epoch's barrier.
//     When non-nil, the caller should snapshot state and forward the barrier.
func (ba *BarrierAligner) OnMessage(im indexedMessage) (toProcess []Message, alignedBarrier *BarrierSignal) {
	msg := im.Msg

	// Check if this is a barrier signal.
	if msg.IsSignal() && msg.Signal != nil && msg.Signal.SignalType == SignalTypeBarrier && msg.Signal.Barrier != nil {
		return ba.onBarrier(im.InputIndex, msg.Signal.Barrier)
	}

	// If we're aligning and this input has already sent its barrier,
	// buffer the message until alignment completes.
	if ba.aligning && ba.received[im.InputIndex] {
		ba.buffered = append(ba.buffered, msg)
		return nil, nil
	}

	// Normal message from a non-barriered input — process immediately.
	return []Message{msg}, nil
}

// onBarrier handles the arrival of a barrier from a specific input.
func (ba *BarrierAligner) onBarrier(inputIndex int, barrier *BarrierSignal) ([]Message, *BarrierSignal) {
	// For single-input nodes, no alignment needed.
	if ba.numInputs <= 1 {
		return nil, barrier
	}

	if !ba.aligning {
		// First barrier for this epoch — start alignment.
		ba.aligning = true
		ba.currentEpoch = barrier.Epoch
		ba.currentBarrier = barrier
		ba.received = make(map[int]bool)
		ba.buffered = nil
	}

	ba.received[inputIndex] = true

	if len(ba.received) == ba.numInputs {
		// All inputs aligned! Return buffered messages and the barrier.
		buffered := ba.buffered
		result := ba.currentBarrier

		// Reset alignment state.
		ba.aligning = false
		ba.currentEpoch = 0
		ba.currentBarrier = nil
		ba.received = make(map[int]bool)
		ba.buffered = nil

		return buffered, result
	}

	return nil, nil
}

// IsAligning returns whether the aligner is currently waiting for barrier
// alignment across all inputs.
func (ba *BarrierAligner) IsAligning() bool {
	return ba.aligning
}
