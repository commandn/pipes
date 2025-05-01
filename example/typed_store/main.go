package main

import (
	"context"
	"errors"
	"fmt"
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
	data, err := s.Read(ctx, fetchHandlerId)
	if err != nil {
		return "", err
	}

	content, ok := data.(string)
	if !ok {
		return "", fmt.Errorf("fetch handler data is not a string: %T", data)
	}

	return content, nil
}

func (s *store) GetProcessHandlerResult(ctx context.Context) (map[rune]struct{}, error) {
	data, err := s.Read(ctx, processHandlerId)
	if err != nil {
		return nil, err
	}

	alphabet, ok := data.(map[rune]struct{})
	if !ok {
		return nil, fmt.Errorf("process handler data is not a string: %T", data)
	}

	return alphabet, nil
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
	register := createRegistry(store, runner)

	err := errors.Join(
		register(fetchHandlerId, fetchHandler("https://google.com")),
		register(processHandlerId, processHandler),
		register(notifyHandlerId, notifyHandler),
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

func createRegistry(
	store typedStore,
	runner *pipes.Runner[typedStore],
) func(int, pipes.Handler[typedStore]) error {
	return func(handlerId int, handler pipes.Handler[typedStore]) error {
		if err := store.Register(handlerId); err != nil {
			return err
		}

		if err := runner.Register(handlerId, handler); err != nil {
			return err
		}

		return nil
	}
}
