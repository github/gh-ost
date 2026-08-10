package network

import (
	"context"
	"fmt"

	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	"github.com/testcontainers/testcontainers-go/internal/core"
)

const (
	// FilterByID uses to filter network by identifier.
	FilterByID = "id"

	// FilterByName uses to filter network by name.
	FilterByName = "name"
)

// Get returns a network by its ID.
func Get(ctx context.Context, id string) (network.Summary, error) {
	return get(ctx, FilterByID, id)
}

// GetByName returns a network by its name.
func GetByName(ctx context.Context, name string) (network.Summary, error) {
	return get(ctx, FilterByName, name)
}

func get(ctx context.Context, filter string, value string) (network.Summary, error) {
	var nw network.Summary // initialize to the zero value

	cli, err := core.NewClient(ctx)
	if err != nil {
		return nw, err
	}
	defer cli.Close()

	list, err := cli.NetworkList(ctx, client.NetworkListOptions{
		Filters: make(client.Filters).Add(filter, value),
	})
	if err != nil {
		return nw, fmt.Errorf("failed to list networks: %w", err)
	}

	if len(list.Items) == 0 {
		return nw, fmt.Errorf("network %s not found (filtering by %s)", value, filter)
	}

	return list.Items[0], nil
}
