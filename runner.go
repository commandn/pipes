package pipes

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"maps"
	"sync"
	"sync/atomic"
	"time"
)

var ErrHandlerAlreadyRegistered = errors.New("handler already registered")

type Handler[S Store] func(context.Context, S) (any, error)

// Generic runner
// It would be much more cleaner to implement typed runner without generics
type Runner[S Store] struct {
	handlers map[int]Handler[S]

	statistics   map[int]time.Duration
	statisticsMu sync.Mutex
}

func NewRunner[S Store]() *Runner[S] {
	return &Runner[S]{
		handlers:   make(map[int]Handler[S]),
		statistics: make(map[int]time.Duration),
	}
}

func (r *Runner[S]) Register(id int, h Handler[S], opts ...Option[S]) error {
	if _, ok := r.handlers[id]; ok {
		return ErrHandlerAlreadyRegistered
	}
	r.handlers[id] = wrap(h, opts)
	return nil
}

func (r *Runner[S]) Run(ctx context.Context, s S) error {
	eg := errgroup.Group{}
	ctx, cancelFn := context.WithCancelCause(ctx)
	defer cancelFn(nil)

	var failover atomic.Bool

	for id, handler := range r.handlers {
		eg.Go(func() (err error) {
			defer func(from time.Time) {
				r.statisticsMu.Lock()
				defer r.statisticsMu.Unlock()
				r.statistics[id] = time.Since(from)
			}(time.Now())

			defer func() {
				if recErr := recover(); recErr != nil {
					err = errors.Join(err, fmt.Errorf("panic recover: %v", recErr))
					// TODO: What if r.s.Write() panics again?
					// TODO: We must write state to unblock dependant consumers
					wErr := s.Write(id, nil, err)
					err = errors.Join(err, wErr)
				}
			}()

			d, e := handler(ctx, s)
			if errors.Is(e, ErrCriticalPath) {
				if failover.CompareAndSwap(false, true) {
					cancelFn(e)
				}
				err = errors.Join(err, e)
			}

			err = errors.Join(err, s.Write(id, d, e))
			return err
		})
	}

	return eg.Wait()
}

func (r *Runner[S]) Statistics() map[int]time.Duration {
	r.statisticsMu.Lock()
	defer r.statisticsMu.Unlock()
	return maps.Clone(r.statistics)
}

func wrap[S Store](h Handler[S], opts []Option[S]) Handler[S] {
	if len(opts) == 0 {
		return h
	}

	handler := opts[len(opts)-1](h)
	for i := len(opts) - 2; i >= 0; i-- {
		handler = opts[i](handler)
	}
	return handler
}
