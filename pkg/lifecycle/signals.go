package lifecycle

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

// SignalTrap intercepts OS termination signals and initiates graceful pod draining.
type SignalTrap struct {
	fsm     *FSM
	sigChan chan os.Signal
	stopCh  chan struct{}
}

// NewSignalTrap creates and activates a signal listener.
func NewSignalTrap(fsm *FSM) *SignalTrap {
	trap := &SignalTrap{
		fsm:     fsm,
		sigChan: make(chan os.Signal, 2),
		stopCh:  make(chan struct{}),
	}

	signal.Notify(trap.sigChan, syscall.SIGTERM, syscall.SIGINT)

	go trap.listen()
	return trap
}

func (s *SignalTrap) listen() {
	select {
	case sig := <-s.sigChan:
		reason := ReasonSIGTERM
		if sig == syscall.SIGINT {
			reason = ReasonSIGINT
		}
		log.Printf("[gracegate] trapped signal %v, initiating zero-loss draining sequence...", sig)
		s.fsm.TriggerDrain(reason)
	case <-s.stopCh:
		return
	}
}

// Stop stops intercepting signals.
func (s *SignalTrap) Stop() {
	signal.Stop(s.sigChan)
	close(s.stopCh)
}
