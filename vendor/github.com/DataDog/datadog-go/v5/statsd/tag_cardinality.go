package statsd

import (
	"os"
	"strings"
)

type Parameter interface{}

type Cardinality int

const (
	CardinalityNotSet Cardinality = iota
	CardinalityNone
	CardinalityLow
	CardinalityOrchestrator
	CardinalityHigh
)

func (c Cardinality) isValid() bool {
	return c >= CardinalityNotSet && c <= CardinalityHigh
}

func (c Cardinality) String() string {
	switch c {
	case CardinalityNone:
		return "none"
	case CardinalityLow:
		return "low"
	case CardinalityOrchestrator:
		return "orchestrator"
	case CardinalityHigh:
		return "high"
	}
	return ""
}

// validateCardinality converts a string to Cardinality
func validateCardinality(card string) (Cardinality, bool) {
	card = strings.ToLower(card)
	switch card {
	case "none":
		return CardinalityNone, true
	case "low":
		return CardinalityLow, true
	case "orchestrator":
		return CardinalityOrchestrator, true
	case "high":
		return CardinalityHigh, true
	default:
		return CardinalityNotSet, false
	}
}

// envTagCardinality returns the tag cardinality value from the DD_CARDINALITY/DATADOG_CARDINALITY environment variable.
func envTagCardinality() (Cardinality, bool) {
	// If the user has not provided a value, read the value from the DD_CARDINALITY environment variable.
	if card, ok := validateCardinality(os.Getenv("DD_CARDINALITY")); ok {
		return card, true
	}

	// If DD_CARDINALITY is not set or valid, read the value from the DATADOG_CARDINALITY environment variable.
	if card, ok := validateCardinality(os.Getenv("DATADOG_CARDINALITY")); ok {
		return card, true
	}

	return CardinalityNotSet, false
}

func parameterCardinality(parameters []Parameter, defaultCardinality Cardinality) Cardinality {
	for _, o := range parameters {
		c, ok := o.(Cardinality)
		if ok && c.isValid() {
			return c
		}
	}
	return defaultCardinality
}
