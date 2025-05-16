package pipes

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_Store_Register(t *testing.T) {
	t.Parallel()

	s := NewStore()

	err := s.Register(1)
	require.NoError(t, err)

	err = s.Register(1)
	require.ErrorIs(t, err, ErrStateAlreadyRegistered)

	err = s.Register(2)
	require.NoError(t, err)

	err = s.Register(3)
	require.NoError(t, err)
}

func Test_Store_Write(t *testing.T) {
	t.Parallel()

	s := NewStore()

	err := s.Write(1, nil, nil)
	require.ErrorIs(t, err, ErrStateNotRegistered)

	err = s.Register(1)
	require.NoError(t, err)

	err = s.Write(1, nil, nil)
	require.NoError(t, err)

	require.Panics(t, func() {
		err = s.Write(1, nil, nil)
		require.ErrorContains(t, err, "panic recover")
	})
}

func Test_Store_Read(t *testing.T) {
	t.Parallel()

	s := NewStore()
	ctx := context.Background()

	data, err := s.Read(ctx, 1)
	require.ErrorIs(t, err, ErrStateNotRegistered)
	require.Nil(t, data)

	err = s.Register(1)
	require.NoError(t, err)

	err = s.Write(1, 42, nil)
	require.NoError(t, err)

	data, err = s.Read(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 42, data)
}

func Test_Read(t *testing.T) {
	t.Parallel()

	s := NewStore()
	ctx := context.Background()

	err := s.Register(1)
	require.NoError(t, err)

	err = s.Write(1, 1, nil)
	require.NoError(t, err)

	_, err = Read[string](ctx, s, 1)
	require.ErrorContains(t, err, "invalid type")

	result, err := Read[int](ctx, s, 1)
	require.NoError(t, err)
	require.Equal(t, 1, result)
}

func Test_Read_FailToRead(t *testing.T) {
	t.Parallel()

	s := NewStore()
	ctx, cancelFn := context.WithCancel(context.Background())

	err := s.Register(1)
	require.NoError(t, err)

	err = s.Write(1, 1, nil)
	require.NoError(t, err)

	cancelFn()

	_, err = Read[int](ctx, s, 1)
	require.ErrorIs(t, err, context.Canceled)
}
