package testcontainers

// Validator is an interface that can be implemented by types that need to validate their state.
type Validator interface {
	// Validate validates the state of the type.
	Validate() error
}
