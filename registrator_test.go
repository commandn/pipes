package pipes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_NewRegistrator(t *testing.T) {
	t.Parallel()

	const handlerId1 = 1

	tcs := []struct {
		name        string
		storeIds    []int
		runnerIds   []int
		id          int
		expectedErr error
	}{
		{
			name:        "state already registered",
			storeIds:    []int{handlerId1},
			runnerIds:   []int{},
			id:          handlerId1,
			expectedErr: ErrStateAlreadyRegistered,
		},
		{
			name:        "handler already registered",
			storeIds:    []int{},
			runnerIds:   []int{handlerId1},
			id:          handlerId1,
			expectedErr: ErrHandlerAlreadyRegistered,
		},
		{
			name:        "success",
			storeIds:    []int{},
			runnerIds:   []int{},
			id:          handlerId1,
			expectedErr: nil,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := NewStore()
			r := NewRunner[Store]()
			registrator := NewRegistrator(s, r)

			for _, id := range tc.storeIds {
				require.NoError(t, s.Register(id))
			}

			for _, id := range tc.runnerIds {
				require.NoError(t, r.Register(id, nil))
			}

			if tc.expectedErr == nil {
				require.NoError(t, registrator(tc.id, nil))
			} else {
				require.Error(t, registrator(tc.id, nil))
			}
		})
	}
}
