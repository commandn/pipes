package pipes

import (
	"context"
	"github.com/stretchr/testify/require"
	"io"
	"sync"
	"testing"
	"time"
)

func Test_State_Read_Data(t *testing.T) {
	t.Parallel()

	s := NewState()
	ctx := context.Background()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	consumer := func() {
		defer wg.Done()
		data, err := s.Read(ctx)
		require.NoError(t, err)
		require.Equal(t, 42, data.(int))
	}

	go consumer()
	go consumer()

	// Wait for 2 consumer goroutines to block on Read call
	time.Sleep(time.Millisecond * 50)

	s.Write(42, nil)
	wg.Wait()
}

func Test_State_Read_Error(t *testing.T) {
	t.Parallel()

	s := NewState()
	ctx := context.Background()

	wg := &sync.WaitGroup{}
	wg.Add(2)

	consumer := func() {
		defer wg.Done()
		data, err := s.Read(ctx)
		require.ErrorIs(t, err, io.EOF)
		require.Nil(t, data)
	}

	go consumer()
	go consumer()

	// Wait for 2 consumer goroutines to block on Read call
	time.Sleep(time.Millisecond * 50)

	s.Write(nil, io.EOF)
	wg.Wait()
}

func Test_State_Read_Cancel(t *testing.T) {
	t.Parallel()

	s := NewState()
	ctx, cancelFn := context.WithCancel(context.Background())

	wg := &sync.WaitGroup{}
	wg.Add(2)

	consumer := func() {
		defer wg.Done()
		data, err := s.Read(ctx)
		require.ErrorIs(t, err, ctx.Err())
		require.Nil(t, data)
	}

	go consumer()
	go consumer()

	// Wait for 2 consumer goroutines to block on Read call
	time.Sleep(time.Millisecond * 50)

	cancelFn()
	wg.Wait()
}

func Test_State_Read_Multiple(t *testing.T) {
	t.Parallel()

	s := NewState()
	s.Write(42, nil)

	ctx := context.Background()

	data, err := s.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, 42, data.(int))

	data, err = s.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, 42, data.(int))
}

func Test_State_Write_Multiple(t *testing.T) {
	t.Parallel()

	s := NewState()
	s.Write(42, nil)
	require.Panics(t, func() { s.Write(42, nil) })
}
