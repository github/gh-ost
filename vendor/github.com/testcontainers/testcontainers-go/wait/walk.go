package wait

import (
	"errors"
	"slices"
)

var (
	// ErrVisitStop is used as a return value from [VisitFunc] to stop the walk.
	// It is not returned as an error by any function.
	ErrVisitStop = errors.New("stop the walk")

	// Deprecated: use [ErrVisitStop] instead.
	VisitStop = ErrVisitStop

	// ErrVisitRemove is used as a return value from [VisitFunc] to have the current node removed.
	// It is not returned as an error by any function.
	ErrVisitRemove = errors.New("remove this strategy")

	// Deprecated: use [ErrVisitRemove] instead.
	VisitRemove = ErrVisitRemove
)

// VisitFunc is a function that visits a strategy node.
// If it returns [ErrVisitStop], the walk stops.
// If it returns [ErrVisitRemove], the current node is removed.
type VisitFunc func(root Strategy) error

// Walk walks the strategies tree and calls the visit function for each node.
func Walk(root *Strategy, visit VisitFunc) error {
	if root == nil {
		return errors.New("root strategy is nil")
	}

	if err := walk(root, visit); err != nil {
		if errors.Is(err, ErrVisitRemove) || errors.Is(err, ErrVisitStop) {
			return nil
		}
		return err
	}

	return nil
}

// walk walks the strategies tree and calls the visit function for each node.
// It returns an error if the visit function returns an error.
func walk(root *Strategy, visit VisitFunc) error {
	if *root == nil {
		// No strategy.
		return nil
	}

	// Allow the visit function to customize the behaviour of the walk before visiting the children.
	if err := visit(*root); err != nil {
		if errors.Is(err, ErrVisitRemove) {
			*root = nil
		}

		return err
	}

	switch s := (*root).(type) {
	case *MultiStrategy:
		if err := walkAndMutate(&s.Strategies, visit); err != nil {
			return err
		}
	case *AnyMultiStrategy:
		if err := walkAndMutate(&s.Strategies, visit); err != nil {
			return err
		}
	}

	return nil
}

func walkAndMutate(strategies *[]Strategy, visit VisitFunc) error {
	for i := 0; i < len(*strategies); {
		if err := walk(&(*strategies)[i], visit); err != nil {
			if errors.Is(err, ErrVisitRemove) {
				*strategies = slices.Delete(*strategies, i, i+1)
				if errors.Is(err, VisitStop) {
					return VisitStop
				}
				continue
			}
			return err
		}
		i++
	}
	return nil
}
