package testcontainers

import (
	"context"
	"fmt"

	"github.com/moby/moby/client"
	specs "github.com/opencontainers/image-spec/specs-go/v1"
)

// ImageInfo represents summary information of an image
type ImageInfo struct {
	ID   string
	Name string
}

type saveImageOptions struct {
	dockerSaveOpts []client.ImageSaveOption
	platforms      []specs.Platform
}

type SaveImageOption func(*saveImageOptions) error

type pullImageOptions struct {
	dockerPullOpts client.ImagePullOptions
}

type PullImageOption func(*pullImageOptions) error

// ResolveSaveImageOptions applies save image options and returns the platform to
// use for a coordinated containerd import, if exactly one platform was requested.
func ResolveSaveImageOptions(opts ...SaveImageOption) (*specs.Platform, error) {
	saveOpts := saveImageOptions{}

	for _, opt := range opts {
		if err := opt(&saveOpts); err != nil {
			return nil, fmt.Errorf("applying save image option: %w", err)
		}
	}

	switch len(saveOpts.platforms) {
	case 0:
		return nil, nil
	case 1:
		return &saveOpts.platforms[0], nil
	default:
		return nil, fmt.Errorf("at most one platform is supported, got %d", len(saveOpts.platforms))
	}
}

// ImageProvider allows manipulating images
type ImageProvider interface {
	ListImages(context.Context) ([]ImageInfo, error)
	SaveImages(context.Context, string, ...string) error
	SaveImagesWithOpts(context.Context, string, []string, ...SaveImageOption) error
	PullImage(context.Context, string) error
	PullImageWithOpts(context.Context, string, ...PullImageOption) error
}
