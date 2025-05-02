package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/foobarbazmeow/pipes"
)

const (
	fibonacciHandlerId = 0
	squareHandlerId    = 1
)

func fibonacciHandler(n int) pipes.Handler[pipes.Store] {
	return func(context.Context, pipes.Store) (any, error) {
		a, b := 0, 1
		for i := 2; i <= n; i++ {
			a, b = b, a+b
		}
		return b, nil
	}
}

func squareHandler(n int) pipes.Handler[pipes.Store] {
	return func(context.Context, pipes.Store) (any, error) {
		result := 0
		for i := 2; i <= n; i++ {
			result += i * i
		}
		return result, nil
	}
}

func main() {
	store := pipes.NewStore()
	runner := pipes.NewRunner[pipes.Store]()
	registrator := pipes.NewRegistrator(store, runner)

	err := errors.Join(
		registrator(fibonacciHandlerId, fibonacciHandler(10)),
		registrator(squareHandlerId, squareHandler(10)),
	)
	if err != nil {
		slog.Error("fail to register handler", "err", err)
		os.Exit(1)
	}

	ctx := context.Background()
	if err = runner.Run(ctx, store); err != nil {
		slog.Error("fail to run pipeline", "err", err)
		os.Exit(1)
	}

	fibonacciResult, err := store.Read(ctx, fibonacciHandlerId)
	slog.Info("fibonacci handler", "result", fibonacciResult, "err", err)

	squareResult, err := store.Read(ctx, squareHandlerId)
	slog.Info("square handler", "result", squareResult, "err", err)
}
