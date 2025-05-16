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
	fetchGoogleHandlerId  = 0
	fetchAmazonHandlerId  = 1
	fetchOpenAIHandlerId  = 2
	processCloudHandlerId = 3
	processAIHandlerId    = 4
	notifyHandlerId       = 5
)

func fetchHandler(url string) pipes.Handler[pipes.Store] {
	return func(context.Context, pipes.Store) (any, error) {
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

func processCloudHandler(ctx context.Context, store pipes.Store) (any, error) {
	googleContent, err := pipes.Read[string](ctx, store, fetchGoogleHandlerId)
	if err != nil {
		return nil, err
	}

	amazonContent, err := pipes.Read[string](ctx, store, fetchAmazonHandlerId)
	if err != nil {
		return nil, err
	}

	alphabet := map[rune]struct{}{}
	for _, r := range googleContent + amazonContent {
		alphabet[r] = struct{}{}
	}

	return alphabet, nil
}

func processAIHandler(ctx context.Context, store pipes.Store) (any, error) {
	openAIContent, err := pipes.Read[string](ctx, store, fetchOpenAIHandlerId)
	if err != nil {
		return nil, err
	}

	alphabet := map[rune]struct{}{}
	for _, r := range openAIContent {
		alphabet[r] = struct{}{}
	}

	return alphabet, nil
}

func notifyHandler(ctx context.Context, store pipes.Store) (any, error) {
	cloudAlphabet, err := pipes.Read[map[rune]struct{}](ctx, store, processCloudHandlerId)
	if err != nil {
		return nil, err
	}

	aiAlphabet, err := pipes.Read[map[rune]struct{}](ctx, store, processAIHandlerId)
	if err != nil {
		return nil, err
	}

	slog.Info("alphabet", "length", len(cloudAlphabet)+len(aiAlphabet))

	return nil, nil
}

func main() {
	store := pipes.NewStore()
	runner := pipes.NewRunner[pipes.Store]()
	registrator := pipes.NewRegistrator(store, runner)

	err := errors.Join(
		registrator(fetchGoogleHandlerId, fetchHandler("https://google.com")),
		registrator(fetchAmazonHandlerId, fetchHandler("https://amazon.com")),
		registrator(fetchOpenAIHandlerId, fetchHandler("https://openai.com")),
		registrator(processCloudHandlerId, processCloudHandler),
		registrator(processAIHandlerId, processAIHandler),
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
