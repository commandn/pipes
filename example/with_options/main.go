package main

import (
	"context"
	"errors"
	"github.com/foobarbazmeow/pipes"
	"log/slog"
	"os"
	"time"
)

const (
	infiniteHandler1Id = 0
	infiniteHandler2Id = 1
	infiniteHandler3Id = 2
)

func infiniteHandler(ctx context.Context, _ pipes.Store) (any, error) {
	xs := make([]int64, 0, 4)
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return xs, ctx.Err()
		case <-time.After(time.Second):
			xs = append(xs, time.Now().Unix())
		}
	}
}

func main() {
	store := pipes.NewStore()
	runner := pipes.NewRunner[pipes.Store]()

	var err error
	err = errors.Join(err, registerHandler(store, runner, infiniteHandler1Id, infiniteHandler, pipes.WithTimeout[pipes.Store](time.Second*1)))
	err = errors.Join(err, registerHandler(store, runner, infiniteHandler2Id, infiniteHandler, pipes.WithTimeout[pipes.Store](time.Second*2)))
	err = errors.Join(err, registerHandler(store, runner, infiniteHandler3Id, infiniteHandler, pipes.WithTimeout[pipes.Store](time.Second*3)))

	if err != nil {
		slog.Error("fail to register handler", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	if err = runner.Run(ctx, store); err != nil {
		slog.Error("fail to run pipeline", "err", err)
		os.Exit(1)
	}

	infiniteHandler1Result, err := store.Read(ctx, infiniteHandler1Id)
	slog.Info("infiniteHandler1Id", "result", infiniteHandler1Result, "err", err)

	infiniteHandler2Result, err := store.Read(ctx, infiniteHandler2Id)
	slog.Info("infiniteHandler2Id", "result", infiniteHandler2Result, "err", err)

	infiniteHandler3Result, err := store.Read(ctx, infiniteHandler3Id)
	slog.Info("infiniteHandler3Id", "result", infiniteHandler3Result, "err", err)
}

func registerHandler(
	store pipes.Store,
	runner *pipes.Runner[pipes.Store],
	handlerId int,
	handler pipes.Handler[pipes.Store],
	opts ...pipes.Option[pipes.Store],
) error {
	if err := store.Register(handlerId); err != nil {
		return err
	}

	if err := runner.Register(handlerId, handler, opts...); err != nil {
		return err
	}

	return nil
}
