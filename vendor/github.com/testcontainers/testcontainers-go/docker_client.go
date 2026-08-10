package testcontainers

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/moby/moby/client"

	"github.com/testcontainers/testcontainers-go/internal"
	"github.com/testcontainers/testcontainers-go/internal/core"
	"github.com/testcontainers/testcontainers-go/log"
)

// DockerClient is a wrapper around the docker client that is used by testcontainers-go.
// It implements the SystemAPIClient interface in order to cache the docker info and reuse it.
type DockerClient struct {
	*client.Client // client is embedded into our own client
}

var (
	// dockerInfo stores the docker info to be reused in the Info method
	dockerInfo     client.SystemInfoResult
	dockerInfoSet  bool
	dockerInfoLock sync.Mutex
)

// implements SystemAPIClient interface
var _ client.SystemAPIClient = &DockerClient{}

// Events returns a channel to listen to events that happen to the docker daemon.
func (c *DockerClient) Events(ctx context.Context, options client.EventsListOptions) client.EventsResult {
	return c.Client.Events(ctx, options)
}

// Info returns information about the docker server. The result of Info is cached
// and reused every time Info is called.
// It will also print out the docker server info, and the resolved Docker paths, to the default logger.
func (c *DockerClient) Info(ctx context.Context, options client.InfoOptions) (client.SystemInfoResult, error) {
	dockerInfoLock.Lock()
	defer dockerInfoLock.Unlock()
	if dockerInfoSet {
		return dockerInfo, nil
	}

	res, err := c.Client.Info(ctx, options)
	if err != nil {
		return res, fmt.Errorf("failed to retrieve docker info: %w", err)
	}
	dockerInfo = res
	dockerInfoSet = true

	infoMessage := `%v - Connected to docker: 
  Server Version: %v
  API Version: %v
  Operating System: %v
  Total Memory: %v MB%s
  Testcontainers for Go Version: v%s
  Resolved Docker Host: %s
  Resolved Docker Socket Path: %s
  Test SessionID: %s
  Test ProcessID: %s
`
	infoLabels := ""
	if len(dockerInfo.Info.Labels) > 0 {
		infoLabels = `
  Labels:`
		var infoLabelsSb72 strings.Builder
		for _, lb := range dockerInfo.Info.Labels {
			infoLabelsSb72.WriteString("\n    " + lb)
		}
		infoLabels += infoLabelsSb72.String()
	}

	host, err := core.ExtractDockerHost(ctx)
	if err != nil {
		return dockerInfo, err
	}
	log.Printf(infoMessage, packagePath,
		dockerInfo.Info.ServerVersion,
		c.ClientVersion(),
		dockerInfo.Info.OperatingSystem, dockerInfo.Info.MemTotal/1024/1024,
		infoLabels,
		internal.Version,
		host,
		core.MustExtractDockerSocket(ctx),
		core.SessionID(),
		core.ProcessID(),
	)

	return dockerInfo, nil
}

// RegistryLogin logs into a Docker registry.
func (c *DockerClient) RegistryLogin(ctx context.Context, options client.RegistryLoginOptions) (client.RegistryLoginResult, error) {
	return c.Client.RegistryLogin(ctx, options)
}

// DiskUsage returns the disk usage of all images.
func (c *DockerClient) DiskUsage(ctx context.Context, options client.DiskUsageOptions) (client.DiskUsageResult, error) {
	return c.Client.DiskUsage(ctx, options)
}

// Ping pings the docker server.
func (c *DockerClient) Ping(ctx context.Context, options client.PingOptions) (client.PingResult, error) {
	return c.Client.Ping(ctx, options)
}

// Deprecated: Use NewDockerClientWithOpts instead.
func NewDockerClient() (*client.Client, error) {
	cli, err := NewDockerClientWithOpts(context.Background())
	if err != nil {
		return nil, err
	}

	return cli.Client, nil
}

func NewDockerClientWithOpts(ctx context.Context, opt ...client.Opt) (*DockerClient, error) {
	dockerClient, err := core.NewClient(ctx, opt...)
	if err != nil {
		return nil, err
	}

	tcClient := DockerClient{
		Client: dockerClient,
	}

	if _, err = tcClient.Info(ctx, client.InfoOptions{}); err != nil {
		// Fallback to environment, including the original options
		if len(opt) == 0 {
			opt = []client.Opt{client.FromEnv}
		}

		apiClient, err := client.New(opt...)
		if err != nil {
			return nil, err
		}

		tcClient.Client = apiClient
	}
	defer tcClient.Close()

	return &tcClient, nil
}
