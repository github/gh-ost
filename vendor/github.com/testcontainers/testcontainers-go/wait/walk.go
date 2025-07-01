package wait

import (
	"errors"
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

	if s, ok := (*root).(*MultiStrategy); ok {
		var i int
		for range s.Strategies {
			if err := walk(&s.Strategies[i], visit); err != nil {
				if errors.Is(err, ErrVisitRemove) {
					s.Strategies = append(s.Strategies[:i], s.Strategies[i+1:]...)
					if errors.Is(err, VisitStop) {
						return VisitStop
					}
					continue
				}

				return err
			}
			i++
		}
	}

	return nil
}
