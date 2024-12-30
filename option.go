package pipes

import (
	"context"
	"errors"
	"time"
)

var (
	ErrSkip         = errors.New("handler was skipped")
	ErrCriticalPath = errors.New("failure on critical path")
)

type Option[S Store] func(Handler[S]) Handler[S]

func WithTimeout[S Store](timeout time.Duration) Option[S] {
	return func(next Handler[S]) Handler[S] {
		return func(ctx context.Context, s S) (any, error) {
			ctx, cancelFn := context.WithTimeout(ctx, timeout)
			defer cancelFn()
			return next(ctx, s)
		}
	}
}

func WithCondition[S Store](skip bool) Option[S] {
	return func(next Handler[S]) Handler[S] {
		return func(ctx context.Context, s S) (any, error) {
			if skip {
				return nil, ErrSkip
			}
			return next(ctx, s)
		}
	}
}

func WithCriticalPath[S Store]() Option[S] {
	return func(next Handler[S]) Handler[S] {
		return func(ctx context.Context, s S) (any, error) {
			data, err := next(ctx, s)
			if err != nil {
				return data, errors.Join(ErrCriticalPath, err)
			}
			return data, err
		}
	}
}
