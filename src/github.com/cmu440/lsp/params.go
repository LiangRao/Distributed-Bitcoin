// DO NOT MODIFY THIS FILE!

package lsp

import "fmt"

// Default values for LSP parameters.
const (
	DefaultEpochLimit         = 5
	DefaultEpochMillis        = 2000
	DefaultWindowSize         = 1
	DefaultMaxBackOffInterval = 0
)

// Params defines configuration parameters for an LSP client or server.
type Params struct {
	// EpochLimit is the number of epochs that can transpire before declaring a
	// connection to be lost.
	EpochLimit int

	// EpochMillis is the number of milliseconds between epochs.
	EpochMillis int

	// WindowSize is the size of the sliding window (i.e. the max number of
	// non-acknowledged messages that can be sent at a given time).
	WindowSize int

	// MaxBackOffInterval is the maximum interval for exponential backoff.
	// The number of epochs between two epochs that transmit the same packet
	// cannot be larger than the number
	MaxBackOffInterval int
}

// NewParams returns a Params with default field values.
func NewParams() *Params {
	return &Params{
		EpochLimit:         DefaultEpochLimit,
		EpochMillis:        DefaultEpochMillis,
		WindowSize:         DefaultWindowSize,
		MaxBackOffInterval: DefaultMaxBackOffInterval,
	}
}

// String returns a string representation of this params. To pretty-print a
// params, you can pass it to a format string like so:
//     params := NewParams()
//     fmt.Printf("New params: %s\n", params)
func (p *Params) String() string {
	return fmt.Sprintf("[EpochLimit: %d, EpochMillis: %d, WindowSize: %d, MaxBackOffInterval: %d]",
		p.EpochLimit, p.EpochMillis, p.WindowSize, p.MaxBackOffInterval)
}
