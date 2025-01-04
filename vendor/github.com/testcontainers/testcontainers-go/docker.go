package testcontainers

import (
	"archive/tar"
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/containerd/platforms"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/moby/term"
	specs "github.com/opencontainers/image-spec/specs-go/v1"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/internal/config"
	"github.com/testcontainers/testcontainers-go/internal/core"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Implement interfaces
var _ Container = (*DockerContainer)(nil)

const (
	Bridge        = "bridge" // Bridge network name (as well as driver)
	Podman        = "podman"
	ReaperDefault = "reaper_default" // Default network name when bridge is not available
	packagePath   = "github.com/testcontainers/testcontainers-go"
)

var (
	// createContainerFailDueToNameConflictRegex is a regular expression that matches the container is already in use error.
	createContainerFailDueToNameConflictRegex = regexp.MustCompile("Conflict. The container name .* is already in use by container .*")

	// minLogProductionTimeout is the minimum log production timeout.
	minLogProductionTimeout = time.Duration(5 * time.Second)

	// maxLogProductionTimeout is the maximum log production timeout.
	maxLogProductionTimeout = time.Duration(60 * time.Second)

	// errLogProductionStop is the cause for stopping log production.
	errLogProductionStop = errors.New("log production stopped")
)

// DockerContainer represents a container started using Docker
type DockerContainer struct {
	// Container ID from Docker
	ID           string
	WaitingFor   wait.Strategy
	Image        string
	exposedPorts []string // a reference to the container's requested exposed ports. It allows checking they are ready before any wait strategy

	isRunning     bool
	imageWasBuilt bool
	// keepBuiltImage makes Terminate not remove the image if imageWasBuilt.
	keepBuiltImage    bool
	provider          *DockerProvider
	sessionID         string
	terminationSignal chan bool
	consumers         []LogConsumer

	// TODO: Remove locking and wait group once the deprecated StartLogProducer and
	// StopLogProducer have been removed and hence logging can only be started and
	// stopped once.

	// logProductionCancel is used to signal the log production to stop.
	logProductionCancel context.CancelCauseFunc
	logProductionCtx    context.Context

	logProductionTimeout *time.Duration
	logger               Logging
	lifecycleHooks       []ContainerLifecycleHooks

	healthStatus string // container health status, will default to healthStatusNone if no healthcheck is present
}

// SetLogger sets the logger for the container
func (c *DockerContainer) SetLogger(logger Logging) {
	c.logger = logger
}

// SetProvider sets the provider for the container
func (c *DockerContainer) SetProvider(provider *DockerProvider) {
	c.provider = provider
}

// SetTerminationSignal sets the termination signal for the container
func (c *DockerContainer) SetTerminationSignal(signal chan bool) {
	c.terminationSignal = signal
}

func (c *DockerContainer) GetContainerID() string {
	return c.ID
}

func (c *DockerContainer) IsRunning() bool {
	return c.isRunning
}

// Endpoint gets proto://host:port string for the lowest numbered exposed port
// Will returns just host:port if proto is ""
func (c *DockerContainer) Endpoint(ctx context.Context, proto string) (string, error) {
	inspect, err := c.Inspect(ctx)
	if err != nil {
		return "", err
	}

	// Get lowest numbered bound port.
	var lowestPort nat.Port
	for port := range inspect.NetworkSettings.Ports {
		if lowestPort == "" || port.Int() < lowestPort.Int() {
			lowestPort = port
		}
	}

	return c.PortEndpoint(ctx, lowestPort, proto)
}

// PortEndpoint gets proto://host:port string for the given exposed port
// Will returns just host:port if proto is ""
func (c *DockerContainer) PortEndpoint(ctx context.Context, port nat.Port, proto string) (string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}

	outerPort, err := c.MappedPort(ctx, port)
	if err != nil {
		return "", err
	}

	protoFull := ""
	if proto != "" {
		protoFull = fmt.Sprintf("%s://", proto)
	}

	return fmt.Sprintf("%s%s:%s", protoFull, host, outerPort.Port()), nil
}

// Host gets host (ip or name) of the docker daemon where the container port is exposed
// Warning: this is based on your Docker host setting. Will fail if using an SSH tunnel
// You can use the "TESTCONTAINERS_HOST_OVERRIDE" env variable to set this yourself
func (c *DockerContainer) Host(ctx context.Context) (string, error) {
	host, err := c.provider.DaemonHost(ctx)
	if err != nil {
		return "", err
	}
	return host, nil
}

// Inspect gets the raw container info
func (c *DockerContainer) Inspect(ctx context.Context) (*types.ContainerJSON, error) {
	jsonRaw, err := c.inspectRawContainer(ctx)
	if err != nil {
		return nil, err
	}

	return jsonRaw, nil
}

// MappedPort gets externally mapped port for a container port
func (c *DockerContainer) MappedPort(ctx context.Context, port nat.Port) (nat.Port, error) {
	inspect, err := c.Inspect(ctx)
	if err != nil {
		return "", fmt.Errorf("inspect: %w", err)
	}
	if inspect.ContainerJSONBase.HostConfig.NetworkMode == "host" {
		return port, nil
	}

	ports := inspect.NetworkSettings.Ports

	for k, p := range ports {
		if k.Port() != port.Port() {
			continue
		}
		if port.Proto() != "" && k.Proto() != port.Proto() {
			continue
		}
		if len(p) == 0 {
			continue
		}
		return nat.NewPort(k.Proto(), p[0].HostPort)
	}

	return "", errdefs.NotFound(fmt.Errorf("port %q not found", port))
}

// Deprecated: use c.Inspect(ctx).NetworkSettings.Ports instead.
// Ports gets the exposed ports for the container.
func (c *DockerContainer) Ports(ctx context.Context) (nat.PortMap, error) {
	inspect, err := c.Inspect(ctx)
	if err != nil {
		return nil, err
	}
	return inspect.NetworkSettings.Ports, nil
}

// SessionID gets the current session id
func (c *DockerContainer) SessionID() string {
	return c.sessionID
}

// Start will start an already created container
func (c *DockerContainer) Start(ctx context.Context) error {
	err := c.startingHook(ctx)
	if err != nil {
		return fmt.Errorf("starting hook: %w", err)
	}

	if err := c.provider.client.ContainerStart(ctx, c.ID, container.StartOptions{}); err != nil {
		return fmt.Errorf("container start: %w", err)
	}
	defer c.provider.Close()

	err = c.startedHook(ctx)
	if err != nil {
		return fmt.Errorf("started hook: %w", err)
	}

	c.isRunning = true

	err = c.readiedHook(ctx)
	if err != nil {
		return fmt.Errorf("readied hook: %w", err)
	}

	return nil
}

// Stop stops the container.
//
// In case the container fails to stop gracefully within a time frame specified
// by the timeout argument, it is forcefully terminated (killed).
//
// If the timeout is nil, the container's StopTimeout value is used, if set,
// otherwise the engine default. A negative timeout value can be specified,
// meaning no timeout, i.e. no forceful termination is performed.
//
// All hooks are called in the following order:
//   - [ContainerLifecycleHooks.PreStops]
//   - [ContainerLifecycleHooks.PostStops]
//
// If the container is already stopped, the method is a no-op.
func (c *DockerContainer) Stop(ctx context.Context, timeout *time.Duration) error {
	// Note we can't check isRunning here because we allow external creation
	// without exposing the ability to fully initialize the container state.
	// See: https://github.com/testcontainers/testcontainers-go/issues/2667
	// TODO: Add a check for isRunning when the above issue is resolved.
	err := c.stoppingHook(ctx)
	if err != nil {
		return fmt.Errorf("stopping hook: %w", err)
	}

	var options container.StopOptions

	if timeout != nil {
		timeoutSeconds := int(timeout.Seconds())
		options.Timeout = &timeoutSeconds
	}

	if err := c.provider.client.ContainerStop(ctx, c.ID, options); err != nil {
		return fmt.Errorf("container stop: %w", err)
	}

	defer c.provider.Close()

	c.isRunning = false

	err = c.stoppedHook(ctx)
	if err != nil {
		return fmt.Errorf("stopped hook: %w", err)
	}

	return nil
}

// Terminate calls stops and then removes the container including its volumes.
// If its image was built it and all child images are also removed unless
// the [FromDockerfile.KeepImage] on the [ContainerRequest] was set to true.
//
// The following hooks are called in order:
//   - [ContainerLifecycleHooks.PreTerminates]
//   - [ContainerLifecycleHooks.PostTerminates]
func (c *DockerContainer) Terminate(ctx context.Context) error {
	// ContainerRemove hardcodes stop timeout to 3 seconds which is too short
	// to ensure that child containers are stopped so we manually call stop.
	// TODO: make this configurable via a functional option.
	timeout := 10 * time.Second
	err := c.Stop(ctx, &timeout)
	if err != nil && !isCleanupSafe(err) {
		return fmt.Errorf("stop: %w", err)
	}

	select {
	// Close reaper connection if it was attached.
	case c.terminationSignal <- true:
	default:
	}

	defer c.provider.client.Close()

	// TODO: Handle errors from ContainerRemove more correctly, e.g. should we
	// run the terminated hook?
	errs := []error{
		c.terminatingHook(ctx),
		c.provider.client.ContainerRemove(ctx, c.GetContainerID(), container.RemoveOptions{
			RemoveVolumes: true,
			Force:         true,
		}),
		c.terminatedHook(ctx),
	}

	if c.imageWasBuilt && !c.keepBuiltImage {
		_, err := c.provider.client.ImageRemove(ctx, c.Image, image.RemoveOptions{
			Force:         true,
			PruneChildren: true,
		})
		errs = append(errs, err)
	}

	c.sessionID = ""
	c.isRunning = false

	return errors.Join(errs...)
}

// update container raw info
func (c *DockerContainer) inspectRawContainer(ctx context.Context) (*types.ContainerJSON, error) {
	defer c.provider.Close()
	inspect, err := c.provider.client.ContainerInspect(ctx, c.ID)
	if err != nil {
		return nil, err
	}

	return &inspect, nil
}

// Logs will fetch both STDOUT and STDERR from the current container. Returns a
// ReadCloser and leaves it up to the caller to extract what it wants.
func (c *DockerContainer) Logs(ctx context.Context) (io.ReadCloser, error) {
	const streamHeaderSize = 8

	options := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	}

	rc, err := c.provider.client.ContainerLogs(ctx, c.ID, options)
	if err != nil {
		return nil, err
	}
	defer c.provider.Close()

	pr, pw := io.Pipe()
	r := bufio.NewReader(rc)

	go func() {
		lineStarted := true
		for err == nil {
			line, isPrefix, err := r.ReadLine()

			if lineStarted && len(line) >= streamHeaderSize {
				line = line[streamHeaderSize:] // trim stream header
				lineStarted = false
			}
			if !isPrefix {
				lineStarted = true
			}

			_, errW := pw.Write(line)
			if errW != nil {
				return
			}

			if !isPrefix {
				_, errW := pw.Write([]byte("\n"))
				if errW != nil {
					return
				}
			}

			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
		}
	}()

	return pr, nil
}

// Deprecated: use the ContainerRequest.LogConsumerConfig field instead.
func (c *DockerContainer) FollowOutput(consumer LogConsumer) {
	c.followOutput(consumer)
}

// followOutput adds a LogConsumer to be sent logs from the container's
// STDOUT and STDERR
func (c *DockerContainer) followOutput(consumer LogConsumer) {
	c.consumers = append(c.consumers, consumer)
}

// Deprecated: use c.Inspect(ctx).Name instead.
// Name gets the name of the container.
func (c *DockerContainer) Name(ctx context.Context) (string, error) {
	inspect, err := c.Inspect(ctx)
	if err != nil {
		return "", err
	}
	return inspect.Name, nil
}

// State returns container's running state.
func (c *DockerContainer) State(ctx context.Context) (*types.ContainerState, error) {
	inspect, err := c.inspectRawContainer(ctx)
	if err != nil {
		return nil, err
	}
	return inspect.State, nil
}

// Networks gets the names of the networks the container is attached to.
func (c *DockerContainer) Networks(ctx context.Context) ([]string, error) {
	inspect, err := c.Inspect(ctx)
	if err != nil {
		return []string{}, err
	}

	networks := inspect.NetworkSettings.Networks

	n := []string{}

	for k := range networks {
		n = append(n, k)
	}

	return n, nil
}

// ContainerIP gets the IP address of the primary network within the container.
func (c *DockerContainer) ContainerIP(ctx context.Context) (string, error) {
	inspect, err := c.Inspect(ctx)
	if err != nil {
		return "", err
	}

	ip := inspect.NetworkSettings.IPAddress
	if ip == "" {
		// use IP from "Networks" if only single network defined
		networks := inspect.NetworkSettings.Networks
		if len(networks) == 1 {
			for _, v := range networks {
				ip = v.IPAddress
			}
		}
	}

	return ip, nil
}

// ContainerIPs gets the IP addresses of all the networks within the container.
func (c *DockerContainer) ContainerIPs(ctx context.Context) ([]string, error) {
	ips := make([]string, 0)

	inspect, err := c.Inspect(ctx)
	if err != nil {
		return nil, err
	}

	networks := inspect.NetworkSettings.Networks
	for _, nw := range networks {
		ips = append(ips, nw.IPAddress)
	}

	return ips, nil
}

// NetworkAliases gets the aliases of the container for the networks it is attached to.
func (c *DockerContainer) NetworkAliases(ctx context.Context) (map[string][]string, error) {
	inspect, err := c.Inspect(ctx)
	if err != nil {
		return map[string][]string{}, err
	}

	networks := inspect.NetworkSettings.Networks

	a := map[string][]string{}

	for k := range networks {
		a[k] = networks[k].Aliases
	}

	return a, nil
}

// Exec executes a command in the current container.
// It returns the exit status of the executed command, an [io.Reader] containing the combined
// stdout and stderr, and any encountered error. Note that reading directly from the [io.Reader]
// may result in unexpected bytes due to custom stream multiplexing headers.
// Use [tcexec.Multiplexed] option to read the combined output without the multiplexing headers.
// Alternatively, to separate the stdout and stderr from [io.Reader] and interpret these headers properly,
// [github.com/docker/docker/pkg/stdcopy.StdCopy] from the Docker API should be used.
func (c *DockerContainer) Exec(ctx context.Context, cmd []string, options ...tcexec.ProcessOption) (int, io.Reader, error) {
	cli := c.provider.client

	processOptions := tcexec.NewProcessOptions(cmd)

	// processing all the options in a first loop because for the multiplexed option
	// we first need to have a containerExecCreateResponse
	for _, o := range options {
		o.Apply(processOptions)
	}

	response, err := cli.ContainerExecCreate(ctx, c.ID, processOptions.ExecConfig)
	if err != nil {
		return 0, nil, fmt.Errorf("container exec create: %w", err)
	}

	hijack, err := cli.ContainerExecAttach(ctx, response.ID, container.ExecAttachOptions{})
	if err != nil {
		return 0, nil, fmt.Errorf("container exec attach: %w", err)
	}

	processOptions.Reader = hijack.Reader

	// second loop to process the multiplexed option, as now we have a reader
	// from the created exec response.
	for _, o := range options {
		o.Apply(processOptions)
	}

	var exitCode int
	for {
		execResp, err := cli.ContainerExecInspect(ctx, response.ID)
		if err != nil {
			return 0, nil, fmt.Errorf("container exec inspect: %w", err)
		}

		if !execResp.Running {
			exitCode = execResp.ExitCode
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	return exitCode, processOptions.Reader, nil
}

type FileFromContainer struct {
	underlying *io.ReadCloser
	tarreader  *tar.Reader
}

func (fc *FileFromContainer) Read(b []byte) (int, error) {
	return (*fc.tarreader).Read(b)
}

func (fc *FileFromContainer) Close() error {
	return (*fc.underlying).Close()
}

func (c *DockerContainer) CopyFileFromContainer(ctx context.Context, filePath string) (io.ReadCloser, error) {
	r, _, err := c.provider.client.CopyFromContainer(ctx, c.ID, filePath)
	if err != nil {
		return nil, err
	}
	defer c.provider.Close()

	tarReader := tar.NewReader(r)

	// if we got here we have exactly one file in the TAR-stream
	// so we advance the index by one so the next call to Read will start reading it
	_, err = tarReader.Next()
	if err != nil {
		return nil, err
	}

	ret := &FileFromContainer{
		underlying: &r,
		tarreader:  tarReader,
	}

	return ret, nil
}

// CopyDirToContainer copies the contents of a directory to a parent path in the container. This parent path must exist in the container first
// as we cannot create it
func (c *DockerContainer) CopyDirToContainer(ctx context.Context, hostDirPath string, containerParentPath string, fileMode int64) error {
	dir, err := isDir(hostDirPath)
	if err != nil {
		return err
	}

	if !dir {
		// it's not a dir: let the consumer to handle an error
		return fmt.Errorf("path %s is not a directory", hostDirPath)
	}

	buff, err := tarDir(hostDirPath, fileMode)
	if err != nil {
		return err
	}

	// create the directory under its parent
	parent := filepath.Dir(containerParentPath)

	err = c.provider.client.CopyToContainer(ctx, c.ID, parent, buff, container.CopyToContainerOptions{})
	if err != nil {
		return err
	}
	defer c.provider.Close()

	return nil
}

func (c *DockerContainer) CopyFileToContainer(ctx context.Context, hostFilePath string, containerFilePath string, fileMode int64) error {
	dir, err := isDir(hostFilePath)
	if err != nil {
		return err
	}

	if dir {
		return c.CopyDirToContainer(ctx, hostFilePath, containerFilePath, fileMode)
	}

	f, err := os.Open(hostFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}

	// In Go 1.22 os.File is always an io.WriterTo. However, testcontainers
	// currently allows Go 1.21, so we need to trick the compiler a little.
	var file fs.File = f
	return c.copyToContainer(ctx, func(tw io.Writer) error {
		// Attempt optimized writeTo, implemented in linux
		if wt, ok := file.(io.WriterTo); ok {
			_, err := wt.WriteTo(tw)
			return err
		}
		_, err := io.Copy(tw, f)
		return err
	}, info.Size(), containerFilePath, fileMode)
}

// CopyToContainer copies fileContent data to a file in container
func (c *DockerContainer) CopyToContainer(ctx context.Context, fileContent []byte, containerFilePath string, fileMode int64) error {
	return c.copyToContainer(ctx, func(tw io.Writer) error {
		_, err := tw.Write(fileContent)
		return err
	}, int64(len(fileContent)), containerFilePath, fileMode)
}

func (c *DockerContainer) copyToContainer(ctx context.Context, fileContent func(tw io.Writer) error, fileContentSize int64, containerFilePath string, fileMode int64) error {
	buffer, err := tarFile(containerFilePath, fileContent, fileContentSize, fileMode)
	if err != nil {
		return err
	}

	err = c.provider.client.CopyToContainer(ctx, c.ID, "/", buffer, container.CopyToContainerOptions{})
	if err != nil {
		return err
	}
	defer c.provider.Close()

	return nil
}

// logConsumerWriter is a writer that writes to a LogConsumer.
type logConsumerWriter struct {
	log       Log
	consumers []LogConsumer
}

// newLogConsumerWriter creates a new logConsumerWriter for logType that sends messages to all consumers.
func newLogConsumerWriter(logType string, consumers []LogConsumer) *logConsumerWriter {
	return &logConsumerWriter{
		log:       Log{LogType: logType},
		consumers: consumers,
	}
}

// Write writes the p content to all consumers.
func (lw logConsumerWriter) Write(p []byte) (int, error) {
	lw.log.Content = p
	for _, consumer := range lw.consumers {
		consumer.Accept(lw.log)
	}
	return len(p), nil
}

type LogProductionOption func(*DockerContainer)

// WithLogProductionTimeout is a functional option that sets the timeout for the log production.
// If the timeout is lower than 5s or greater than 60s it will be set to 5s or 60s respectively.
func WithLogProductionTimeout(timeout time.Duration) LogProductionOption {
	return func(c *DockerContainer) {
		c.logProductionTimeout = &timeout
	}
}

// Deprecated: use the ContainerRequest.LogConsumerConfig field instead.
func (c *DockerContainer) StartLogProducer(ctx context.Context, opts ...LogProductionOption) error {
	return c.startLogProduction(ctx, opts...)
}

// startLogProduction will start a concurrent process that will continuously read logs
// from the container and will send them to each added LogConsumer.
//
// Default log production timeout is 5s. It is used to set the context timeout
// which means that each log-reading loop will last at up to the specified timeout.
//
// Use functional option WithLogProductionTimeout() to override default timeout. If it's
// lower than 5s and greater than 60s it will be set to 5s or 60s respectively.
func (c *DockerContainer) startLogProduction(ctx context.Context, opts ...LogProductionOption) error {
	for _, opt := range opts {
		opt(c)
	}

	// Validate the log production timeout.
	switch {
	case c.logProductionTimeout == nil:
		c.logProductionTimeout = &minLogProductionTimeout
	case *c.logProductionTimeout < minLogProductionTimeout:
		c.logProductionTimeout = &minLogProductionTimeout
	case *c.logProductionTimeout > maxLogProductionTimeout:
		c.logProductionTimeout = &maxLogProductionTimeout
	}

	// Setup the log writers.
	stdout := newLogConsumerWriter(StdoutLog, c.consumers)
	stderr := newLogConsumerWriter(StderrLog, c.consumers)

	// Setup the log production context which will be used to stop the log production.
	c.logProductionCtx, c.logProductionCancel = context.WithCancelCause(ctx)

	go func() {
		err := c.logProducer(stdout, stderr)
		// Set context cancel cause, if not already set.
		c.logProductionCancel(err)
	}()

	return nil
}

// logProducer read logs from the container and writes them to stdout, stderr until either:
//   - logProductionCtx is done
//   - A fatal error occurs
//   - No more logs are available
func (c *DockerContainer) logProducer(stdout, stderr io.Writer) error {
	// Clean up idle client connections.
	defer c.provider.Close()

	// Setup the log options, start from the beginning.
	options := container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	}

	for {
		timeoutCtx, cancel := context.WithTimeout(c.logProductionCtx, *c.logProductionTimeout)
		defer cancel()

		err := c.copyLogs(timeoutCtx, stdout, stderr, options)
		switch {
		case err == nil:
			// No more logs available.
			return nil
		case c.logProductionCtx.Err() != nil:
			// Log production was stopped or caller context is done.
			return nil
		case timeoutCtx.Err() != nil, errors.Is(err, net.ErrClosed):
			// Timeout or client connection closed, retry.
		default:
			// Unexpected error, retry.
			Logger.Printf("Unexpected error reading logs: %v", err)
		}

		// Retry from the last log received.
		now := time.Now()
		options.Since = fmt.Sprintf("%d.%09d", now.Unix(), int64(now.Nanosecond()))
	}
}

// copyLogs copies logs from the container to stdout and stderr.
func (c *DockerContainer) copyLogs(ctx context.Context, stdout, stderr io.Writer, options container.LogsOptions) error {
	rc, err := c.provider.client.ContainerLogs(ctx, c.GetContainerID(), options)
	if err != nil {
		return fmt.Errorf("container logs: %w", err)
	}
	defer rc.Close()

	if _, err = stdcopy.StdCopy(stdout, stderr, rc); err != nil {
		return fmt.Errorf("stdcopy: %w", err)
	}

	return nil
}

// Deprecated: it will be removed in the next major release.
func (c *DockerContainer) StopLogProducer() error {
	return c.stopLogProduction()
}

// stopLogProduction will stop the concurrent process that is reading logs
// and sending them to each added LogConsumer
func (c *DockerContainer) stopLogProduction() error {
	if c.logProductionCancel == nil {
		return nil
	}

	// Signal the log production to stop.
	c.logProductionCancel(errLogProductionStop)

	if err := context.Cause(c.logProductionCtx); err != nil {
		switch {
		case errors.Is(err, errLogProductionStop):
			// Log production was stopped.
			return nil
		case errors.Is(err, context.DeadlineExceeded),
			errors.Is(err, context.Canceled):
			// Parent context is done.
			return nil
		default:
			return err
		}
	}

	return nil
}

// GetLogProductionErrorChannel exposes the only way for the consumer
// to be able to listen to errors and react to them.
func (c *DockerContainer) GetLogProductionErrorChannel() <-chan error {
	if c.logProductionCtx == nil {
		return nil
	}

	errCh := make(chan error, 1)
	go func() {
		<-c.logProductionCtx.Done()
		errCh <- context.Cause(c.logProductionCtx)
	}()
	return errCh
}

// DockerNetwork represents a network started using Docker
type DockerNetwork struct {
	ID                string // Network ID from Docker
	Driver            string
	Name              string
	provider          *DockerProvider
	terminationSignal chan bool
}

// Remove is used to remove the network. It is usually triggered by as defer function.
func (n *DockerNetwork) Remove(ctx context.Context) error {
	select {
	// close reaper if it was created
	case n.terminationSignal <- true:
	default:
	}

	defer n.provider.Close()

	return n.provider.client.NetworkRemove(ctx, n.ID)
}

func (n *DockerNetwork) SetTerminationSignal(signal chan bool) {
	n.terminationSignal = signal
}

// DockerProvider implements the ContainerProvider interface
type DockerProvider struct {
	*DockerProviderOptions
	client    client.APIClient
	host      string
	hostCache string
	config    config.Config
}

// Client gets the docker client used by the provider
func (p *DockerProvider) Client() client.APIClient {
	return p.client
}

// Close closes the docker client used by the provider
func (p *DockerProvider) Close() error {
	if p.client == nil {
		return nil
	}

	return p.client.Close()
}

// SetClient sets the docker client to be used by the provider
func (p *DockerProvider) SetClient(c client.APIClient) {
	p.client = c
}

var _ ContainerProvider = (*DockerProvider)(nil)

// BuildImage will build and image from context and Dockerfile, then return the tag
func (p *DockerProvider) BuildImage(ctx context.Context, img ImageBuildInfo) (string, error) {
	var buildOptions types.ImageBuildOptions
	resp, err := backoff.RetryNotifyWithData(
		func() (types.ImageBuildResponse, error) {
			var err error
			buildOptions, err = img.BuildOptions()
			if err != nil {
				return types.ImageBuildResponse{}, backoff.Permanent(fmt.Errorf("build options: %w", err))
			}
			defer tryClose(buildOptions.Context) // release resources in any case

			resp, err := p.client.ImageBuild(ctx, buildOptions.Context, buildOptions)
			if err != nil {
				if isPermanentClientError(err) {
					return types.ImageBuildResponse{}, backoff.Permanent(fmt.Errorf("build image: %w", err))
				}
				return types.ImageBuildResponse{}, err
			}
			defer p.Close()

			return resp, nil
		},
		backoff.WithContext(backoff.NewExponentialBackOff(), ctx),
		func(err error, duration time.Duration) {
			p.Logger.Printf("Failed to build image: %s, will retry", err)
		},
	)
	if err != nil {
		return "", err // Error is already wrapped.
	}
	defer resp.Body.Close()

	output := io.Discard
	if img.ShouldPrintBuildLog() {
		output = os.Stderr
	}

	// Always process the output, even if it is not printed
	// to ensure that errors during the build process are
	// correctly handled.
	termFd, isTerm := term.GetFdInfo(output)
	if err = jsonmessage.DisplayJSONMessagesStream(resp.Body, output, termFd, isTerm, nil); err != nil {
		return "", fmt.Errorf("build image: %w", err)
	}

	// the first tag is the one we want
	return buildOptions.Tags[0], nil
}

// CreateContainer fulfils a request for a container without starting it
func (p *DockerProvider) CreateContainer(ctx context.Context, req ContainerRequest) (con Container, err error) { //nolint:nonamedreturns // Needed for error checking.
	// defer the close of the Docker client connection the soonest
	defer p.Close()

	// Make sure that bridge network exists
	// In case it is disabled we will create reaper_default network
	if p.DefaultNetwork == "" {
		p.DefaultNetwork, err = p.getDefaultNetwork(ctx, p.client)
		if err != nil {
			return nil, err
		}
	}

	// If default network is not bridge make sure it is attached to the request
	// as container won't be attached to it automatically
	// in case of Podman the bridge network is called 'podman' as 'bridge' would conflict
	if p.DefaultNetwork != p.defaultBridgeNetworkName {
		isAttached := false
		for _, net := range req.Networks {
			if net == p.DefaultNetwork {
				isAttached = true
				break
			}
		}

		if !isAttached {
			req.Networks = append(req.Networks, p.DefaultNetwork)
		}
	}

	imageName := req.Image

	env := []string{}
	for envKey, envVar := range req.Env {
		env = append(env, envKey+"="+envVar)
	}

	if req.Labels == nil {
		req.Labels = make(map[string]string)
	}

	var termSignal chan bool
	// the reaper does not need to start a reaper for itself
	isReaperContainer := strings.HasSuffix(imageName, config.ReaperDefaultImage)
	if !p.config.RyukDisabled && !isReaperContainer {
		r, err := spawner.reaper(context.WithValue(ctx, core.DockerHostContextKey, p.host), core.SessionID(), p)
		if err != nil {
			return nil, fmt.Errorf("reaper: %w", err)
		}

		termSignal, err := r.Connect()
		if err != nil {
			return nil, fmt.Errorf("reaper connect: %w", err)
		}

		// Cleanup on error.
		defer func() {
			if err != nil {
				termSignal <- true
			}
		}()
	}

	if err = req.Validate(); err != nil {
		return nil, err
	}

	// always append the hub substitutor after the user-defined ones
	req.ImageSubstitutors = append(req.ImageSubstitutors, newPrependHubRegistry(p.config.HubImageNamePrefix))

	var platform *specs.Platform

	if req.ShouldBuildImage() {
		imageName, err = p.BuildImage(ctx, &req)
		if err != nil {
			return nil, err
		}
	} else {
		for _, is := range req.ImageSubstitutors {
			modifiedTag, err := is.Substitute(imageName)
			if err != nil {
				return nil, fmt.Errorf("failed to substitute image %s with %s: %w", imageName, is.Description(), err)
			}

			if modifiedTag != imageName {
				p.Logger.Printf("✍🏼 Replacing image with %s. From: %s to %s\n", is.Description(), imageName, modifiedTag)
				imageName = modifiedTag
			}
		}

		if req.ImagePlatform != "" {
			p, err := platforms.Parse(req.ImagePlatform)
			if err != nil {
				return nil, fmt.Errorf("invalid platform %s: %w", req.ImagePlatform, err)
			}
			platform = &p
		}

		var shouldPullImage bool

		if req.AlwaysPullImage {
			shouldPullImage = true // If requested always attempt to pull image
		} else {
			img, _, err := p.client.ImageInspectWithRaw(ctx, imageName)
			if err != nil {
				if client.IsErrNotFound(err) {
					shouldPullImage = true
				} else {
					return nil, err
				}
			}
			if platform != nil && (img.Architecture != platform.Architecture || img.Os != platform.OS) {
				shouldPullImage = true
			}
		}

		if shouldPullImage {
			pullOpt := image.PullOptions{
				Platform: req.ImagePlatform, // may be empty
			}
			if err := p.attemptToPullImage(ctx, imageName, pullOpt); err != nil {
				return nil, err
			}
		}
	}

	if !isReaperContainer {
		// Add the labels that identify this as a testcontainers container and
		// allow the reaper to terminate it if requested.
		AddGenericLabels(req.Labels)
	}

	dockerInput := &container.Config{
		Entrypoint: req.Entrypoint,
		Image:      imageName,
		Env:        env,
		Labels:     req.Labels,
		Cmd:        req.Cmd,
		Hostname:   req.Hostname,
		User:       req.User,
		WorkingDir: req.WorkingDir,
	}

	hostConfig := &container.HostConfig{
		Privileged: req.Privileged,
		ShmSize:    req.ShmSize,
		Tmpfs:      req.Tmpfs,
	}

	networkingConfig := &network.NetworkingConfig{}

	// default hooks include logger hook and pre-create hook
	defaultHooks := []ContainerLifecycleHooks{
		DefaultLoggingHook(p.Logger),
		defaultPreCreateHook(p, dockerInput, hostConfig, networkingConfig),
		defaultCopyFileToContainerHook(req.Files),
		defaultLogConsumersHook(req.LogConsumerCfg),
		defaultReadinessHook(),
	}

	// in the case the container needs to access a local port
	// we need to forward the local port to the container
	if len(req.HostAccessPorts) > 0 {
		// a container lifecycle hook will be added, which will expose the host ports to the container
		// using a SSHD server running in a container. The SSHD server will be started and will
		// forward the host ports to the container ports.
		sshdForwardPortsHook, err := exposeHostPorts(ctx, &req, req.HostAccessPorts...)
		if err != nil {
			return nil, fmt.Errorf("expose host ports: %w", err)
		}

		defaultHooks = append(defaultHooks, sshdForwardPortsHook)
	}

	req.LifecycleHooks = []ContainerLifecycleHooks{combineContainerHooks(defaultHooks, req.LifecycleHooks)}

	err = req.creatingHook(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.client.ContainerCreate(ctx, dockerInput, hostConfig, networkingConfig, platform, req.Name)
	if err != nil {
		return nil, fmt.Errorf("container create: %w", err)
	}

	// #248: If there is more than one network specified in the request attach newly created container to them one by one
	if len(req.Networks) > 1 {
		for _, n := range req.Networks[1:] {
			nw, err := p.GetNetwork(ctx, NetworkRequest{
				Name: n,
			})
			if err == nil {
				endpointSetting := network.EndpointSettings{
					Aliases: req.NetworkAliases[n],
				}
				err = p.client.NetworkConnect(ctx, nw.ID, resp.ID, &endpointSetting)
				if err != nil {
					return nil, fmt.Errorf("network connect: %w", err)
				}
			}
		}
	}

	c := &DockerContainer{
		ID:                resp.ID,
		WaitingFor:        req.WaitingFor,
		Image:             imageName,
		imageWasBuilt:     req.ShouldBuildImage(),
		keepBuiltImage:    req.ShouldKeepBuiltImage(),
		sessionID:         core.SessionID(),
		exposedPorts:      req.ExposedPorts,
		provider:          p,
		terminationSignal: termSignal,
		logger:            p.Logger,
		lifecycleHooks:    req.LifecycleHooks,
	}

	err = c.createdHook(ctx)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (p *DockerProvider) findContainerByName(ctx context.Context, name string) (*types.Container, error) {
	if name == "" {
		return nil, nil
	}

	// Note that, 'name' filter will use regex to find the containers
	filter := filters.NewArgs(filters.Arg("name", fmt.Sprintf("^%s$", name)))
	containers, err := p.client.ContainerList(ctx, container.ListOptions{Filters: filter})
	if err != nil {
		return nil, err
	}
	defer p.Close()

	if len(containers) > 0 {
		return &containers[0], nil
	}
	return nil, nil
}

func (p *DockerProvider) waitContainerCreation(ctx context.Context, name string) (*types.Container, error) {
	return backoff.RetryNotifyWithData(
		func() (*types.Container, error) {
			c, err := p.findContainerByName(ctx, name)
			if err != nil {
				if !errdefs.IsNotFound(err) && isPermanentClientError(err) {
					return nil, backoff.Permanent(err)
				}
				return nil, err
			}

			if c == nil {
				return nil, errdefs.NotFound(fmt.Errorf("container %s not found", name))
			}
			return c, nil
		},
		backoff.WithContext(backoff.NewExponentialBackOff(), ctx),
		func(err error, duration time.Duration) {
			if errdefs.IsNotFound(err) {
				return
			}
			p.Logger.Printf("Waiting for container. Got an error: %v; Retrying in %d seconds", err, duration/time.Second)
		},
	)
}

func (p *DockerProvider) ReuseOrCreateContainer(ctx context.Context, req ContainerRequest) (con Container, err error) { //nolint:nonamedreturns // Needed for error check.
	c, err := p.findContainerByName(ctx, req.Name)
	if err != nil {
		return nil, err
	}
	if c == nil {
		createdContainer, err := p.CreateContainer(ctx, req)
		if err == nil {
			return createdContainer, nil
		}
		if !createContainerFailDueToNameConflictRegex.MatchString(err.Error()) {
			return nil, err
		}
		c, err = p.waitContainerCreation(ctx, req.Name)
		if err != nil {
			return nil, err
		}
	}

	sessionID := core.SessionID()

	var termSignal chan bool
	if !p.config.RyukDisabled {
		r, err := spawner.reaper(context.WithValue(ctx, core.DockerHostContextKey, p.host), sessionID, p)
		if err != nil {
			return nil, fmt.Errorf("reaper: %w", err)
		}

		termSignal, err := r.Connect()
		if err != nil {
			return nil, fmt.Errorf("reaper connect: %w", err)
		}

		// Cleanup on error.
		defer func() {
			if err != nil {
				termSignal <- true
			}
		}()
	}

	// default hooks include logger hook and pre-create hook
	defaultHooks := []ContainerLifecycleHooks{
		DefaultLoggingHook(p.Logger),
		defaultReadinessHook(),
		defaultLogConsumersHook(req.LogConsumerCfg),
	}

	dc := &DockerContainer{
		ID:                c.ID,
		WaitingFor:        req.WaitingFor,
		Image:             c.Image,
		sessionID:         sessionID,
		exposedPorts:      req.ExposedPorts,
		provider:          p,
		terminationSignal: termSignal,
		logger:            p.Logger,
		lifecycleHooks:    []ContainerLifecycleHooks{combineContainerHooks(defaultHooks, req.LifecycleHooks)},
	}

	err = dc.startedHook(ctx)
	if err != nil {
		return nil, err
	}

	dc.isRunning = true

	err = dc.readiedHook(ctx)
	if err != nil {
		return nil, err
	}

	return dc, nil
}

// attemptToPullImage tries to pull the image while respecting the ctx cancellations.
// Besides, if the image cannot be pulled due to ErrorNotFound then no need to retry but terminate immediately.
func (p *DockerProvider) attemptToPullImage(ctx context.Context, tag string, pullOpt image.PullOptions) error {
	registry, imageAuth, err := DockerImageAuth(ctx, tag)
	if err != nil {
		p.Logger.Printf("Failed to get image auth for %s. Setting empty credentials for the image: %s. Error is: %s", registry, tag, err)
	} else {
		// see https://github.com/docker/docs/blob/e8e1204f914767128814dca0ea008644709c117f/engine/api/sdk/examples.md?plain=1#L649-L657
		encodedJSON, err := json.Marshal(imageAuth)
		if err != nil {
			p.Logger.Printf("Failed to marshal image auth. Setting empty credentials for the image: %s. Error is: %s", tag, err)
		} else {
			pullOpt.RegistryAuth = base64.URLEncoding.EncodeToString(encodedJSON)
		}
	}

	var pull io.ReadCloser
	err = backoff.RetryNotify(
		func() error {
			pull, err = p.client.ImagePull(ctx, tag, pullOpt)
			if err != nil {
				if isPermanentClientError(err) {
					return backoff.Permanent(err)
				}
				return err
			}
			defer p.Close()

			return nil
		},
		backoff.WithContext(backoff.NewExponentialBackOff(), ctx),
		func(err error, duration time.Duration) {
			p.Logger.Printf("Failed to pull image: %s, will retry", err)
		},
	)
	if err != nil {
		return err
	}
	defer pull.Close()

	// download of docker image finishes at EOF of the pull request
	_, err = io.ReadAll(pull)
	return err
}

// Health measure the healthiness of the provider. Right now we leverage the
// docker-client Info endpoint to see if the daemon is reachable.
func (p *DockerProvider) Health(ctx context.Context) error {
	_, err := p.client.Info(ctx)
	defer p.Close()

	return err
}

// RunContainer takes a RequestContainer as input and it runs a container via the docker sdk
func (p *DockerProvider) RunContainer(ctx context.Context, req ContainerRequest) (Container, error) {
	c, err := p.CreateContainer(ctx, req)
	if err != nil {
		return nil, err
	}

	if err := c.Start(ctx); err != nil {
		return c, fmt.Errorf("%w: could not start container", err)
	}

	return c, nil
}

// Config provides the TestcontainersConfig read from $HOME/.testcontainers.properties or
// the environment variables
func (p *DockerProvider) Config() TestcontainersConfig {
	return TestcontainersConfig{
		Host:           p.config.Host,
		TLSVerify:      p.config.TLSVerify,
		CertPath:       p.config.CertPath,
		RyukDisabled:   p.config.RyukDisabled,
		RyukPrivileged: p.config.RyukPrivileged,
		Config:         p.config,
	}
}

// DaemonHost gets the host or ip of the Docker daemon where ports are exposed on
// Warning: this is based on your Docker host setting. Will fail if using an SSH tunnel
// You can use the "TESTCONTAINERS_HOST_OVERRIDE" env variable to set this yourself
func (p *DockerProvider) DaemonHost(ctx context.Context) (string, error) {
	return daemonHost(ctx, p)
}

func daemonHost(ctx context.Context, p *DockerProvider) (string, error) {
	if p.hostCache != "" {
		return p.hostCache, nil
	}

	host, exists := os.LookupEnv("TESTCONTAINERS_HOST_OVERRIDE")
	if exists {
		p.hostCache = host
		return p.hostCache, nil
	}

	// infer from Docker host
	daemonURL, err := url.Parse(p.client.DaemonHost())
	if err != nil {
		return "", err
	}
	defer p.Close()

	switch daemonURL.Scheme {
	case "http", "https", "tcp":
		p.hostCache = daemonURL.Hostname()
	case "unix", "npipe":
		if core.InAContainer() {
			ip, err := p.GetGatewayIP(ctx)
			if err != nil {
				ip, err = core.DefaultGatewayIP()
				if err != nil {
					ip = "localhost"
				}
			}
			p.hostCache = ip
		} else {
			p.hostCache = "localhost"
		}
	default:
		return "", errors.New("could not determine host through env or docker host")
	}

	return p.hostCache, nil
}

// Deprecated: use network.New instead
// CreateNetwork returns the object representing a new network identified by its name
func (p *DockerProvider) CreateNetwork(ctx context.Context, req NetworkRequest) (net Network, err error) { //nolint:nonamedreturns // Needed for error check.
	// defer the close of the Docker client connection the soonest
	defer p.Close()

	// Make sure that bridge network exists
	// In case it is disabled we will create reaper_default network
	if p.DefaultNetwork == "" {
		if p.DefaultNetwork, err = p.getDefaultNetwork(ctx, p.client); err != nil {
			return nil, err
		}
	}

	if req.Labels == nil {
		req.Labels = make(map[string]string)
	}

	nc := network.CreateOptions{
		Driver:     req.Driver,
		Internal:   req.Internal,
		EnableIPv6: req.EnableIPv6,
		Attachable: req.Attachable,
		Labels:     req.Labels,
		IPAM:       req.IPAM,
	}

	sessionID := core.SessionID()

	var termSignal chan bool
	if !p.config.RyukDisabled {
		r, err := spawner.reaper(context.WithValue(ctx, core.DockerHostContextKey, p.host), sessionID, p)
		if err != nil {
			return nil, fmt.Errorf("reaper: %w", err)
		}

		termSignal, err := r.Connect()
		if err != nil {
			return nil, fmt.Errorf("reaper connect: %w", err)
		}

		// Cleanup on error.
		defer func() {
			if err != nil {
				termSignal <- true
			}
		}()
	}

	// add the labels that the reaper will use to terminate the network to the request
	core.AddDefaultLabels(sessionID, req.Labels)

	response, err := p.client.NetworkCreate(ctx, req.Name, nc)
	if err != nil {
		return &DockerNetwork{}, fmt.Errorf("create network: %w", err)
	}

	n := &DockerNetwork{
		ID:                response.ID,
		Driver:            req.Driver,
		Name:              req.Name,
		terminationSignal: termSignal,
		provider:          p,
	}

	return n, nil
}

// GetNetwork returns the object representing the network identified by its name
func (p *DockerProvider) GetNetwork(ctx context.Context, req NetworkRequest) (network.Inspect, error) {
	networkResource, err := p.client.NetworkInspect(ctx, req.Name, network.InspectOptions{
		Verbose: true,
	})
	if err != nil {
		return network.Inspect{}, err
	}

	return networkResource, err
}

func (p *DockerProvider) GetGatewayIP(ctx context.Context) (string, error) {
	// Use a default network as defined in the DockerProvider
	if p.DefaultNetwork == "" {
		var err error
		p.DefaultNetwork, err = p.getDefaultNetwork(ctx, p.client)
		if err != nil {
			return "", err
		}
	}
	nw, err := p.GetNetwork(ctx, NetworkRequest{Name: p.DefaultNetwork})
	if err != nil {
		return "", err
	}

	var ip string
	for _, cfg := range nw.IPAM.Config {
		if cfg.Gateway != "" {
			ip = cfg.Gateway
			break
		}
	}
	if ip == "" {
		return "", errors.New("Failed to get gateway IP from network settings")
	}

	return ip, nil
}

func (p *DockerProvider) getDefaultNetwork(ctx context.Context, cli client.APIClient) (string, error) {
	// Get list of available networks
	networkResources, err := cli.NetworkList(ctx, network.ListOptions{})
	if err != nil {
		return "", err
	}

	reaperNetwork := ReaperDefault

	reaperNetworkExists := false

	for _, net := range networkResources {
		if net.Name == p.defaultBridgeNetworkName {
			return p.defaultBridgeNetworkName, nil
		}

		if net.Name == reaperNetwork {
			reaperNetworkExists = true
		}
	}

	// Create a bridge network for the container communications
	if !reaperNetworkExists {
		_, err = cli.NetworkCreate(ctx, reaperNetwork, network.CreateOptions{
			Driver:     Bridge,
			Attachable: true,
			Labels:     GenericLabels(),
		})
		// If the network already exists, we can ignore the error as that can
		// happen if we are running multiple tests in parallel and we only
		// need to ensure that the network exists.
		if err != nil && !errdefs.IsConflict(err) {
			return "", err
		}
	}

	return reaperNetwork, nil
}

// containerFromDockerResponse builds a Docker container struct from the response of the Docker API
func containerFromDockerResponse(ctx context.Context, response types.Container) (*DockerContainer, error) {
	provider, err := NewDockerProvider()
	if err != nil {
		return nil, err
	}

	ctr := DockerContainer{}

	ctr.ID = response.ID
	ctr.WaitingFor = nil
	ctr.Image = response.Image
	ctr.imageWasBuilt = false

	ctr.logger = provider.Logger
	ctr.lifecycleHooks = []ContainerLifecycleHooks{
		DefaultLoggingHook(ctr.logger),
	}
	ctr.provider = provider

	ctr.sessionID = core.SessionID()
	ctr.consumers = []LogConsumer{}
	ctr.isRunning = response.State == "running"

	// the termination signal should be obtained from the reaper
	ctr.terminationSignal = nil

	// populate the raw representation of the container
	jsonRaw, err := ctr.inspectRawContainer(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect raw container: %w", err)
	}

	// the health status of the container, if any
	if health := jsonRaw.State.Health; health != nil {
		ctr.healthStatus = health.Status
	}

	return &ctr, nil
}

// ListImages list images from the provider. If an image has multiple Tags, each tag is reported
// individually with the same ID and same labels
func (p *DockerProvider) ListImages(ctx context.Context) ([]ImageInfo, error) {
	images := []ImageInfo{}

	imageList, err := p.client.ImageList(ctx, image.ListOptions{})
	if err != nil {
		return images, fmt.Errorf("listing images %w", err)
	}

	for _, img := range imageList {
		for _, tag := range img.RepoTags {
			images = append(images, ImageInfo{ID: img.ID, Name: tag})
		}
	}

	return images, nil
}

// SaveImages exports a list of images as an uncompressed tar
func (p *DockerProvider) SaveImages(ctx context.Context, output string, images ...string) error {
	outputFile, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("opening output file %w", err)
	}
	defer func() {
		_ = outputFile.Close()
	}()

	imageReader, err := p.client.ImageSave(ctx, images)
	if err != nil {
		return fmt.Errorf("saving images %w", err)
	}
	defer func() {
		_ = imageReader.Close()
	}()

	// Attempt optimized readFrom, implemented in linux
	_, err = outputFile.ReadFrom(imageReader)
	if err != nil {
		return fmt.Errorf("writing images to output %w", err)
	}

	return nil
}

// PullImage pulls image from registry
func (p *DockerProvider) PullImage(ctx context.Context, img string) error {
	return p.attemptToPullImage(ctx, img, image.PullOptions{})
}

var permanentClientErrors = []func(error) bool{
	errdefs.IsNotFound,
	errdefs.IsInvalidParameter,
	errdefs.IsUnauthorized,
	errdefs.IsForbidden,
	errdefs.IsNotImplemented,
	errdefs.IsSystem,
}

func isPermanentClientError(err error) bool {
	for _, isErrFn := range permanentClientErrors {
		if isErrFn(err) {
			return true
		}
	}
	return false
}

func tryClose(r io.Reader) {
	rc, ok := r.(io.Closer)
	if ok {
		_ = rc.Close()
	}
}
