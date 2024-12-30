package pipes

import (
	"context"
)

type State struct {
	done chan struct{}

	data any
	err  error
}

func NewState() *State {
	return &State{done: make(chan struct{})}
}

func (s *State) Read(ctx context.Context) (any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.done:
	}
	return s.data, s.err
}

func (s *State) Write(data any, err error) {
	s.data, s.err = data, err
	close(s.done)
}
