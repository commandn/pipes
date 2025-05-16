package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"

	"github.com/foobarbazmeow/pipes"
)

const (
	fetchHandlerId   = 0
	processHandlerId = 1
	notifyHandlerId  = 2
)

type typedStore interface {
	pipes.Store
	GetFetchHandlerResult(context.Context) (string, error)
	GetProcessHandlerResult(ctx context.Context) (map[rune]struct{}, error)
}

type store struct {
	pipes.Store
}

func newStore(s pipes.Store) typedStore {
	return &store{s}
}

func (s *store) GetFetchHandlerResult(ctx context.Context) (string, error) {
	return pipes.Read[string](ctx, s, fetchHandlerId)
}

func (s *store) GetProcessHandlerResult(ctx context.Context) (map[rune]struct{}, error) {
	return pipes.Read[map[rune]struct{}](ctx, s, processHandlerId)
}

func fetchHandler(url string) pipes.Handler[typedStore] {
	return func(context.Context, typedStore) (any, error) {
		response, err := http.Get(url)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()

		bytes, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, err
		}

		return string(bytes), nil
	}
}

func processHandler(ctx context.Context, store typedStore) (any, error) {
	content, err := store.GetFetchHandlerResult(ctx)
	if err != nil {
		return nil, err
	}

	alphabet := map[rune]struct{}{}
	for _, r := range content {
		alphabet[r] = struct{}{}
	}

	return alphabet, nil
}

func notifyHandler(ctx context.Context, store typedStore) (any, error) {
	alphabet, err := store.GetProcessHandlerResult(ctx)
	if err != nil {
		return nil, err
	}

	slog.Info("alphabet", "length", len(alphabet))

	return nil, nil
}

func main() {
	store := newStore(pipes.NewStore())
	runner := pipes.NewRunner[typedStore]()
	registrator := pipes.NewRegistrator(store, runner)

	err := errors.Join(
		registrator(fetchHandlerId, fetchHandler("https://google.com")),
		registrator(processHandlerId, processHandler),
		registrator(notifyHandlerId, notifyHandler),
	)
	if err != nil {
		slog.Error("fail to register handler", "err", err)
		os.Exit(1)
	}

	if err = runner.Run(context.Background(), store); err != nil {
		slog.Error("fail to run pipeline", "err", err)
		os.Exit(1)
	}
}
