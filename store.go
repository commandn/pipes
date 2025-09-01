package pipes

import (
	"context"
	"errors"
	"fmt"
)

type Store interface {
	Read(ctx context.Context, id int) (any, error)
	Write(id int, data any, err error) error
	Register(id int) error
}

var (
	ErrStateNotRegistered     = errors.New("state not registered")
	ErrStateAlreadyRegistered = errors.New("state already registered")
)

type store struct {
	m map[int]*State
}

func NewStore() Store {
	return &store{make(map[int]*State)}
}

func (s *store) Register(id int) error {
	if _, ok := s.m[id]; ok {
		return ErrStateAlreadyRegistered
	}
	s.m[id] = NewState()
	return nil
}

func (s *store) Read(ctx context.Context, id int) (any, error) {
	if state, ok := s.m[id]; ok {
		data, err := state.Read(ctx)
		return data, err
	}
	return nil, ErrStateNotRegistered
}

func (s *store) Write(id int, data any, err error) error {
	if state, ok := s.m[id]; ok {
		state.Write(data, err)
		return nil
	}
	return ErrStateNotRegistered
}

func Read[T any](ctx context.Context, s Store, handlerId int) (T, error) {
	untyped, err := s.Read(ctx, handlerId)
	if untyped == nil {
		return *new(T), err
	}

	result, ok := untyped.(T)
	if !ok {
		return *new(T), fmt.Errorf("invalid type %T for data from handler %d", *new(T), handlerId)
	}

	return result, err
}
