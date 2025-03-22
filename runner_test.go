package pipes

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_Runner_Register(t *testing.T) {
	t.Parallel()

	const handlerId = 41

	handler := func(context.Context, Store) (any, error) {
		return "foobar", nil
	}

	r := NewRunner[Store]()

	err := r.Register(handlerId, handler)
	require.NoError(t, err)

	err = r.Register(handlerId, handler)
	require.ErrorIs(t, err, ErrHandlerAlreadyRegistered)
}

func Test_Runner_Run_SuccessHandler(t *testing.T) {
	t.Parallel()

	const handlerId = 42

	handler := func(context.Context, Store) (any, error) {
		return "foobar", nil
	}

	s := NewStore()
	err := s.Register(handlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()
	err = r.Register(handlerId, handler)
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.NoError(t, err)

	data, err := s.Read(context.Background(), handlerId)
	require.Equal(t, "foobar", data)
	require.NoError(t, err)
}

func Test_Runner_Run_ErrorHandler(t *testing.T) {
	t.Parallel()

	const handlerId = 43

	handler := func(context.Context, Store) (any, error) {
		return nil, fmt.Errorf("error in handler")
	}

	s := NewStore()
	err := s.Register(handlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()
	err = r.Register(handlerId, handler)
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.NoError(t, err)

	data, err := s.Read(context.Background(), handlerId)
	require.Nil(t, data)
	require.ErrorContains(t, err, "error in handler")
}

func Test_Runner_Run_PanicHandler(t *testing.T) {
	t.Parallel()

	const handlerId = 44

	handler := func(context.Context, Store) (any, error) {
		panic("panic in handler")
	}

	s := NewStore()
	err := s.Register(handlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()
	err = r.Register(handlerId, handler)
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.ErrorContains(t, err, "panic recover")

	data, err := s.Read(context.Background(), handlerId)
	require.Nil(t, data)
	require.ErrorContains(t, err, "panic recover")
}

func Test_Runner_Run_InfiniteHandler(t *testing.T) {
	t.Parallel()

	const handlerId = 45

	handler := func(ctx context.Context, _ Store) (any, error) {
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Second):
				// artificial delay
			}
		}
	}

	s := NewStore()
	err := s.Register(handlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()
	err = r.Register(handlerId, handler, WithTimeout[Store](time.Second*3))
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.NoError(t, err)

	data, err := s.Read(context.Background(), handlerId)
	require.Nil(t, data)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_Runner_Run_SkipHandler(t *testing.T) {
	t.Parallel()

	const handlerId = 46

	handler := func(context.Context, Store) (any, error) {
		return "foobar", nil
	}

	tcs := []struct {
		name string
		skip bool
	}{
		{"skip handler", true},
		{"do not skip handler", false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStore()
			err := s.Register(handlerId)
			require.NoError(t, err)

			r := NewRunner[Store]()
			err = r.Register(handlerId, handler, WithCondition[Store](tc.skip))
			require.NoError(t, err)

			err = r.Run(context.Background(), s)
			require.NoError(t, err)

			data, err := s.Read(context.Background(), handlerId)
			if tc.skip {
				require.Nil(t, data)
				require.ErrorIs(t, err, ErrSkip)
			} else {
				require.NotNil(t, data)
				require.NoError(t, err)
			}
		})
	}
}

func Test_Runner_Run_NoStateHandler(t *testing.T) {
	t.Parallel()

	const handlerId = 47

	handler := func(context.Context, Store) (any, error) {
		return nil, nil
	}

	s := NewStore()
	err := s.Register(handlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()

	err = r.Register(handlerId, handler)
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.NoError(t, err)

	data, err := s.Read(context.Background(), handlerId)
	require.Nil(t, data)
	require.NoError(t, err)
}

func Test_Runner_Run_CriticalPathHandler_ErrorInHandler(t *testing.T) {
	t.Parallel()

	const handlerId = 48

	handler := func(context.Context, Store) (any, error) {
		return nil, fmt.Errorf("error in handler on critical path")
	}

	tcs := []struct {
		name             string
		withCriticalPath bool
	}{
		{"with critical path", true},
		{"without critical path", false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStore()
			err := s.Register(handlerId)
			require.NoError(t, err)

			var opts []Option[Store]
			if tc.withCriticalPath {
				opts = append(opts, WithCriticalPath[Store]())
			}

			r := NewRunner[Store]()
			err = r.Register(handlerId, handler, opts...)
			require.NoError(t, err)

			err = r.Run(context.Background(), s)
			if tc.withCriticalPath {
				require.ErrorIs(t, err, ErrCriticalPath)

				data, err := s.Read(context.Background(), handlerId)
				require.Nil(t, data)
				require.ErrorIs(t, err, ErrCriticalPath)
			} else {
				require.NoError(t, err)

				data, err := s.Read(context.Background(), handlerId)
				require.Nil(t, data)
				require.NotErrorIs(t, err, ErrCriticalPath)
			}
		})
	}
}

func Test_Runner_Run_CriticalPathHandler_NoErrorInHandler(t *testing.T) {
	t.Parallel()

	const handlerId = 48

	handler := func(context.Context, Store) (any, error) {
		return "foobar", nil
	}

	tcs := []struct {
		name             string
		withCriticalPath bool
	}{
		{"with critical path", true},
		{"without critical path", false},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStore()
			err := s.Register(handlerId)
			require.NoError(t, err)

			var opts []Option[Store]
			if tc.withCriticalPath {
				opts = append(opts, WithCriticalPath[Store]())
			}

			r := NewRunner[Store]()
			err = r.Register(handlerId, handler, opts...)
			require.NoError(t, err)

			err = r.Run(context.Background(), s)
			require.NoError(t, err)

			data, err := s.Read(context.Background(), handlerId)
			require.Equal(t, "foobar", data)
			require.NoError(t, err)
		})
	}
}

func Test_Runner_Run_WithRunAfter(t *testing.T) {
	t.Parallel()

	const handlerId1 = 45
	const handlerId2 = 46
	const handlerId3 = 47

	handler := func(d time.Duration) Handler[Store] {
		return func(context.Context, Store) (any, error) {
			time.Sleep(d)
			return nil, nil
		}
	}

	s := NewStore()
	err := errors.Join(
		s.Register(handlerId1),
		s.Register(handlerId2),
		s.Register(handlerId3),
	)
	require.NoError(t, err)

	r := NewRunner[Store]()
	err = errors.Join(
		r.Register(handlerId1, handler(time.Millisecond*100)),
		r.Register(handlerId2, handler(time.Millisecond*200), WithRunAfter[Store](handlerId1)),
		r.Register(handlerId3, handler(time.Millisecond*300), WithRunAfter[Store](handlerId2)),
	)
	require.NoError(t, err)

	start := time.Now()
	err = r.Run(context.Background(), s)
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), time.Millisecond*600)
}

func Test_Runner_Statistics(t *testing.T) {
	t.Parallel()

	const (
		handlerId1 = 1
		handlerId2 = 2
		handlerId3 = 3
	)

	handler := func(delay time.Duration) Handler[Store] {
		return func(context.Context, Store) (any, error) {
			time.Sleep(delay)
			return nil, nil
		}
	}

	s := NewStore()
	err := s.Register(handlerId1)
	require.NoError(t, err)
	err = s.Register(handlerId2)
	require.NoError(t, err)
	err = s.Register(handlerId3)
	require.NoError(t, err)

	r := NewRunner[Store]()
	err = r.Register(handlerId1, handler(time.Second))
	require.NoError(t, err)
	err = r.Register(handlerId2, handler(time.Second*2))
	require.NoError(t, err)
	err = r.Register(handlerId3, handler(time.Second*3))
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.NoError(t, err)

	statistics := r.Statistics()
	require.Contains(t, statistics, handlerId1)
	require.GreaterOrEqual(t, statistics[handlerId1], time.Second)
	require.Contains(t, statistics, handlerId2)
	require.GreaterOrEqual(t, statistics[handlerId2], time.Second*2)
	require.Contains(t, statistics, handlerId3)
	require.GreaterOrEqual(t, statistics[handlerId3], time.Second*3)
}

func Test_wrap(t *testing.T) {
	t.Parallel()

	var result []int

	const handlerId = 49

	handler := func(context.Context, Store) (any, error) {
		result = append(result, 0)
		return nil, nil
	}

	option := func(value int) Option[Store] {
		return func(next Handler[Store]) Handler[Store] {
			return func(ctx context.Context, s Store) (any, error) {
				result = append(result, value)
				return next(ctx, s)
			}
		}
	}

	s := NewStore()
	err := s.Register(handlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()
	err = r.Register(handlerId, handler, option(3), option(2), option(1))
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.NoError(t, err)

	data, err := s.Read(context.Background(), handlerId)
	require.Nil(t, data)
	require.NoError(t, err)

	require.Equal(t, []int{3, 2, 1, 0}, result)
}
