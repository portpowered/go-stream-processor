package engine

import "time"

// WatermarkHolder tracks per-input watermarks for a node and computes the
// output watermark as the minimum across all inputs. At merge nodes, this
// ensures the output watermark only advances when the slowest input has
// caught up. For single-input nodes, watermarks pass through directly.
//
// Idle inputs are excluded from the minimum computation, allowing watermarks
// to advance even when some inputs have no new data.
type WatermarkHolder struct {
	numInputs int
	// perInput tracks the latest watermark from each input.
	perInput []inputWatermark
	// current is the last emitted output watermark.
	current time.Time
}

// inputWatermark tracks watermark state for a single input.
type inputWatermark struct {
	eventTime time.Time
	idle      bool
	received  bool // true once at least one watermark has been received
}

// NewWatermarkHolder creates a new WatermarkHolder for a node with the given
// number of inputs. If numInputs <= 0, it defaults to 1.
func NewWatermarkHolder(numInputs int) *WatermarkHolder {
	if numInputs <= 0 {
		numInputs = 1
	}
	return &WatermarkHolder{
		numInputs: numInputs,
		perInput:  make([]inputWatermark, numInputs),
	}
}

// Update records a watermark from the given input index and returns the new
// output watermark if it has advanced, or nil if it hasn't.
//
// The output watermark is the minimum EventTime across all non-idle inputs
// that have received at least one watermark. If all inputs are idle, the
// output watermark advances to the maximum across all inputs (since all
// sources are caught up).
func (wh *WatermarkHolder) Update(inputIndex int, wm WatermarkSignal) *WatermarkSignal {
	if inputIndex < 0 || inputIndex >= wh.numInputs {
		return nil
	}

	wh.perInput[inputIndex] = inputWatermark{
		eventTime: wm.EventTime,
		idle:      wm.Idle,
		received:  true,
	}

	// For single-input nodes, always emit if the watermark advances.
	if wh.numInputs == 1 {
		return wh.tryAdvance()
	}

	// For multi-input nodes, only compute min once all inputs have reported.
	for i := 0; i < wh.numInputs; i++ {
		if !wh.perInput[i].received {
			return nil
		}
	}

	return wh.tryAdvance()
}

// tryAdvance computes the output watermark and returns it if it has advanced.
func (wh *WatermarkHolder) tryAdvance() *WatermarkSignal {
	var minTime time.Time
	allIdle := true
	hasNonIdle := false
	initialized := false

	for i := 0; i < wh.numInputs; i++ {
		iw := wh.perInput[i]
		if !iw.received {
			continue
		}

		if !iw.idle {
			allIdle = false
			hasNonIdle = true
			if !initialized || iw.eventTime.Before(minTime) {
				minTime = iw.eventTime
				initialized = true
			}
		}
	}

	// If all inputs are idle, use the max across all inputs.
	if allIdle {
		for i := 0; i < wh.numInputs; i++ {
			iw := wh.perInput[i]
			if !iw.received {
				continue
			}
			if !initialized || iw.eventTime.After(minTime) {
				minTime = iw.eventTime
				initialized = true
			}
		}
		if !initialized {
			return nil
		}
		// Only advance if the watermark has actually moved forward.
		if !minTime.After(wh.current) {
			return nil
		}
		wh.current = minTime
		return &WatermarkSignal{EventTime: minTime, Idle: true}
	}

	if !hasNonIdle || !initialized {
		return nil
	}

	// Only advance if the watermark has actually moved forward.
	if !minTime.After(wh.current) {
		return nil
	}

	wh.current = minTime
	return &WatermarkSignal{EventTime: minTime}
}

// Current returns the current output watermark time.
func (wh *WatermarkHolder) Current() time.Time {
	return wh.current
}
