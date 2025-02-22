package pipes

import (
	"context"
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
