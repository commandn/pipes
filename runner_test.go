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

	const successHandlerId = 42

	successHandler := func(context.Context, Store) (any, error) {
		return "foobar", nil
	}

	s := NewStore()
	err := s.Register(successHandlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()

	err = r.Register(successHandlerId, successHandler)
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.NoError(t, err)
}

func Test_Runner_Run_ErrorHandler(t *testing.T) {
	t.Parallel()

	const errorHandlerId = 43

	errorHandler := func(context.Context, Store) (any, error) {
		return nil, fmt.Errorf("error in handler")
	}

	s := NewStore()
	err := s.Register(errorHandlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()

	err = r.Register(errorHandlerId, errorHandler)
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.NoError(t, err)
}

func Test_Runner_Run_PanicHandler(t *testing.T) {
	t.Parallel()

	const panicHandlerId = 44

	panicHandler := func(context.Context, Store) (any, error) {
		panic("panic in handler")
	}

	s := NewStore()
	err := s.Register(panicHandlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()

	err = r.Register(panicHandlerId, panicHandler)
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.ErrorContains(t, err, "panic recover")
}

func Test_Runner_Run_InfiniteHandler(t *testing.T) {
	t.Parallel()

	const infiniteHandlerId = 45

	infiniteHandler := func(ctx context.Context, _ Store) (any, error) {
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
	err := s.Register(infiniteHandlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()

	err = r.Register(infiniteHandlerId, infiniteHandler, WithTimeout[Store](time.Second*3))
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.NoError(t, err)

	data, err := s.Read(context.Background(), infiniteHandlerId)
	require.Nil(t, data)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func Test_Runner_Run_SkipHandler(t *testing.T) {
	t.Parallel()

	const skipHandlerId = 46

	skipHandler := func(context.Context, Store) (any, error) {
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
			err := s.Register(skipHandlerId)
			require.NoError(t, err)

			r := NewRunner[Store]()
			err = r.Register(skipHandlerId, skipHandler, WithCondition[Store](tc.skip))
			require.NoError(t, err)

			err = r.Run(context.Background(), s)
			require.NoError(t, err)

			data, err := s.Read(context.Background(), skipHandlerId)
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

	const noStateHandlerId = 47

	noStateHandler := func(context.Context, Store) (any, error) {
		return nil, nil
	}

	s := NewStore()
	err := s.Register(noStateHandlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()

	err = r.Register(noStateHandlerId, noStateHandler)
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.NoError(t, err)

	data, err := s.Read(context.Background(), noStateHandlerId)
	require.Nil(t, data)
	require.NoError(t, err)
}

func Test_Runner_Run_CriticalPathHandler(t *testing.T) {
	t.Parallel()

	const criticalPathHandlerId = 48

	criticalPathHandler := func(context.Context, Store) (any, error) {
		return nil, fmt.Errorf("error in handler on critical path")
	}

	s := NewStore()
	err := s.Register(criticalPathHandlerId)
	require.NoError(t, err)

	r := NewRunner[Store]()

	// TODO: add table test to another handler result (not failed)
	err = r.Register(criticalPathHandlerId, criticalPathHandler, WithCriticalPath[Store]())
	require.NoError(t, err)

	err = r.Run(context.Background(), s)
	require.ErrorIs(t, err, ErrCriticalPath)

	data, err := s.Read(context.Background(), criticalPathHandlerId)
	require.Nil(t, data)
	require.ErrorIs(t, err, ErrCriticalPath)
}
