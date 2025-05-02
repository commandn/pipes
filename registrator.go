package pipes

type Registrator[S Store] func(
	handlerId int,
	handler Handler[S],
	opts ...Option[S],
) error

func NewRegistrator[S Store](
	store S,
	runner *Runner[S],
) Registrator[S] {
	return func(handlerId int, handler Handler[S], opts ...Option[S]) error {
		if err := store.Register(handlerId); err != nil {
			return err
		}
		if err := runner.Register(handlerId, handler, opts...); err != nil {
			return err
		}
		return nil
	}
}
