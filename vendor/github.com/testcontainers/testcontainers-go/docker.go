package testcontainers

import (
	"archive/tar"
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/containerd/errdefs"
	"github.com/containerd/platforms"
	"github.com/moby/moby/api/pkg/authconfig"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
	"github.com/moby/moby/client/pkg/jsonmessage"
	specs "github.com/opencontainers/image-spec/specs-go/v1"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/internal/config"
	"github.com/testcontainers/testcontainers-go/internal/core"
	"github.com/testcontainers/testcontainers-go/log"
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
	createContainerFailDueToNameConflictRegex = regexp.MustCompile("[Tt]he container name .* is already in use by .*")

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

	isRunning     atomic.Bool
	imageWasBuilt bool
	// keepBuiltImage makes Terminate not remove the image if imageWasBuilt.
	keepBuiltImage    bool
	provider          *DockerProvider
	sessionID         string
	terminationSignal chan bool
	consumersMtx      sync.Mutex // protects consumers
	consumers         []LogConsumer

	// TODO: Remove locking and wait group once the deprecated StartLogProducer and
	// StopLogProducer have been removed and hence logging can only be started and
	// stopped once.

	// logProductionCancel is used to signal the log production to stop.
	logProductionCancel context.CancelCauseFunc
	logProductionCtx    context.Context
	// logProductionDone is closed when the log production goroutine exits.
	logProductionDone chan struct{}

	logProductionTimeout *time.Duration
	logger               log.Logger
	lifecycleHooks       []ContainerLifecycleHooks

	healthStatus container.HealthStatus // container health status, will default to healthStatusNone if no healthcheck is present
}

// SetLogger sets the logger for the container
func (c *DockerContainer) SetLogger(logger log.Logger) {
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
	return c.isRunning.Load()
}

// Endpoint gets proto://host:port string for the lowest numbered exposed port
// Will returns just host:port if proto is ""
func (c *DockerContainer) Endpoint(ctx context.Context, proto string) (string, error) {
	inspect, err := c.Inspect(ctx)
	if err != nil {
		return "", err
	}

	// Get lowest numbered bound port.
	var lowestPort network.Port
	for port := range inspect.NetworkSettings.Ports {
		if lowestPort.IsZero() || port.Num() < lowestPort.Num() {
			lowestPort = port
		}
	}

	return c.PortEndpoint(ctx, lowestPort.String(), proto)
}

// PortEndpoint gets proto://host:port string for the given exposed port
// It returns proto://host:port or proto://[IPv6host]:port string for the given exposed port.
// It returns just host:port or [IPv6host]:port if proto is blank.
func (c *DockerContainer) PortEndpoint(ctx context.Context, port string, proto string) (string, error) {
	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}

	outerPort, err := c.MappedPort(ctx, port)
	if err != nil {
		return "", err
	}

	hostPort := net.JoinHostPort(host, outerPort.Port())
	if proto == "" {
		return hostPort, nil
	}

	return proto + "://" + hostPort, nil
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
func (c *DockerContainer) Inspect(ctx context.Context) (*container.InspectResponse, error) {
	jsonRaw, err := c.inspectRawContainer(ctx)
	if err != nil {
		return nil, err
	}

	return &jsonRaw.Container, nil
}

// MappedPort gets externally mapped port for a container port
func (c *DockerContainer) MappedPort(ctx context.Context, port string) (network.Port, error) {
	inspect, err := c.Inspect(ctx)
	if err != nil {
		return network.Port{}, fmt.Errorf("inspect: %w", err)
	}
	// The old nat.Port type (a plain string) accepted empty strings:
	// nat.SplitProtoPort("") returns ("", ""), so Port() == "" and
	// no container port matches, yielding "not found".
	// See https://github.com/docker/go-connections/blob/v0.6.0/nat/nat.go#L101-L110
	// Skip parsing here to preserve that behavior and avoid a
	// ParsePort error on empty input.
	var nwPort network.Port
	if port != "" {
		nwPort, err = network.ParsePort(port)
		if err != nil {
			return network.Port{}, err
		}
	}

	if inspect.HostConfig.NetworkMode == "host" {
		return nwPort, nil
	}

	ports := inspect.NetworkSettings.Ports

	for k, p := range ports {
		if k.Num() != nwPort.Num() {
			continue
		}
		if nwPort.Proto() != "" && k.Proto() != nwPort.Proto() {
			continue
		}
		if len(p) == 0 {
			continue
		}
		pNum, _ := strconv.ParseUint(p[0].HostPort, 10, 16)
		hPort, _ := network.PortFrom(uint16(pNum), k.Proto())
		return hPort, nil
	}

	return network.Port{}, errdefs.ErrNotFound.WithMessage(fmt.Sprintf("port %q not found", nwPort))
}

// Deprecated: use c.Inspect(ctx).NetworkSettings.Ports instead.
// Ports gets the exposed ports for the container.
func (c *DockerContainer) Ports(ctx context.Context) (network.PortMap, error) {
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

	if _, err := c.provider.client.ContainerStart(ctx, c.ID, client.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("container start: %w", err)
	}
	defer c.provider.Close()

	err = c.startedHook(ctx)
	if err != nil {
		return fmt.Errorf("started hook: %w", err)
	}

	c.isRunning.Store(true)

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

	var options client.ContainerStopOptions

	if timeout != nil {
		timeoutSeconds := int(timeout.Seconds())
		options.Timeout = &timeoutSeconds
	}

	if _, err := c.provider.client.ContainerStop(ctx, c.ID, options); err != nil {
		return fmt.Errorf("container stop: %w", err)
	}

	defer c.provider.Close()

	c.isRunning.Store(false)

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
//
// Default: timeout is 10 seconds.
func (c *DockerContainer) Terminate(ctx context.Context, opts ...TerminateOption) error {
	if c == nil {
		return nil
	}

	options := NewTerminateOptions(ctx, opts...)
	err := c.Stop(options.Context(), options.StopTimeout())
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
	var errs []error
	errs = append(errs, c.terminatingHook(ctx))
	_, err = c.provider.client.ContainerRemove(ctx, c.GetContainerID(), client.ContainerRemoveOptions{
		RemoveVolumes: true,
		Force:         true,
	})
	errs = append(errs, err)
	errs = append(errs, c.terminatedHook(ctx))

	if c.imageWasBuilt && !c.keepBuiltImage {
		_, err := c.provider.client.ImageRemove(ctx, c.Image, client.ImageRemoveOptions{
			Force:         true,
			PruneChildren: true,
		})
		errs = append(errs, err)
	}

	c.sessionID = ""
	c.isRunning.Store(false)

	if err = options.Cleanup(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

// update container raw info
func (c *DockerContainer) inspectRawContainer(ctx context.Context) (*client.ContainerInspectResult, error) {
	defer c.provider.Close()
	inspect, err := c.provider.client.ContainerInspect(ctx, c.ID, client.ContainerInspectOptions{})
	if err != nil {
		return nil, err
	}

	return &inspect, nil
}

// Logs will fetch both STDOUT and STDERR from the current container. Returns a
// ReadCloser and leaves it up to the caller to extract what it wants.
func (c *DockerContainer) Logs(ctx context.Context) (io.ReadCloser, error) {
	rc, err := c.provider.client.ContainerLogs(ctx, c.ID, client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return nil, err
	}
	defer c.provider.Close()

	resp, err := c.Inspect(ctx)
	if err != nil {
		return nil, err
	}

	if resp.Config.Tty {
		return rc, nil
	}

	return c.parseMultiplexedLogs(rc), nil
}

// parseMultiplexedLogs handles the multiplexed log format used when TTY is disabled
func (c *DockerContainer) parseMultiplexedLogs(rc io.ReadCloser) io.ReadCloser {
	const streamHeaderSize = 8

	pr, pw := io.Pipe()
	r := bufio.NewReader(rc)

	go func() {
		header := make([]byte, streamHeaderSize)
		for {
			_, errH := io.ReadFull(r, header)
			if errH != nil {
				_ = pw.CloseWithError(errH)
				return
			}

			frameSize := binary.BigEndian.Uint32(header[4:])
			if _, err := io.CopyN(pw, r, int64(frameSize)); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
	}()

	return pr
}

// Deprecated: use the ContainerRequest.LogConsumerConfig field instead.
func (c *DockerContainer) FollowOutput(consumer LogConsumer) {
	c.followOutput(consumer)
}

// followOutput adds a LogConsumer to be sent logs from the container's
// STDOUT and STDERR
func (c *DockerContainer) followOutput(consumer LogConsumer) {
	c.consumersMtx.Lock()
	defer c.consumersMtx.Unlock()

	c.consumers = append(c.consumers, consumer)
}

// consumersCopy returns a copy of the current consumers.
func (c *DockerContainer) consumersCopy() []LogConsumer {
	c.consumersMtx.Lock()
	defer c.consumersMtx.Unlock()

	return slices.Clone(c.consumers)
}

// resetConsumers resets the current consumers to the provided ones.
func (c *DockerContainer) resetConsumers(consumers []LogConsumer) {
	c.consumersMtx.Lock()
	defer c.consumersMtx.Unlock()

	c.consumers = c.consumers[:0]
	c.consumers = append(c.consumers, consumers...)
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
func (c *DockerContainer) State(ctx context.Context) (*container.State, error) {
	inspect, err := c.inspectRawContainer(ctx)
	if err != nil {
		return nil, err
	}
	return inspect.Container.State, nil
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

	var ip string
	//  IPAddress is deprecated; use IP from "Networks" if only single network defined
	networks := inspect.NetworkSettings.Networks
	if len(networks) == 1 {
		for _, v := range networks {
			if v.IPAddress.IsValid() {
				ip = v.IPAddress.String()
			}
		}
	}

	return ip, nil
}

// ContainerIPs gets the IP addresses of all the networks within the container.
func (c *DockerContainer) ContainerIPs(ctx context.Context) ([]string, error) {
	inspect, err := c.Inspect(ctx)
	if err != nil {
		return nil, err
	}

	networks := inspect.NetworkSettings.Networks
	ips := make([]string, 0, len(networks))
	for _, nw := range networks {
		if nw.IPAddress.IsValid() {
			ips = append(ips, nw.IPAddress.String())
		}
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

	response, err := cli.ExecCreate(ctx, c.ID, processOptions.ExecConfig)
	if err != nil {
		return 0, nil, fmt.Errorf("container exec create: %w", err)
	}

	hijack, err := cli.ExecAttach(ctx, response.ID, client.ExecAttachOptions{})
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
		execResp, err := cli.ExecInspect(ctx, response.ID, client.ExecInspectOptions{})
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
	r, err := c.provider.client.CopyFromContainer(ctx, c.ID, client.CopyFromContainerOptions{
		SourcePath: filePath,
	})
	if err != nil {
		return nil, err
	}
	defer c.provider.Close()

	tarReader := tar.NewReader(r.Content)

	// if we got here we have exactly one file in the TAR-stream
	// so we advance the index by one so the next call to Read will start reading it
	_, err = tarReader.Next()
	if err != nil {
		return nil, err
	}

	ret := &FileFromContainer{
		underlying: &r.Content,
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
		// it's not a dir: let the consumer handle the error
		return fmt.Errorf("path %s is not a directory", hostDirPath)
	}

	buff, err := tarDir(hostDirPath, fileMode)
	if err != nil {
		return err
	}

	// create the directory under its parent
	parent := filepath.Dir(containerParentPath)

	_, err = c.provider.client.CopyToContainer(ctx, c.ID, client.CopyToContainerOptions{
		DestinationPath: parent,
		Content:         buff,
	})
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

	_, err = c.provider.client.CopyToContainer(ctx, c.ID, client.CopyToContainerOptions{
		DestinationPath: "/",
		Content:         buffer,
	})
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

	consumers := c.consumersCopy()
	stdout := newLogConsumerWriter(StdoutLog, consumers)
	stderr := newLogConsumerWriter(StderrLog, consumers)

	// Setup the log production context which will be used to stop the log production.
	c.logProductionCtx, c.logProductionCancel = context.WithCancelCause(ctx)
	c.logProductionDone = make(chan struct{})

	// We capture context cancel function to avoid data race with multiple
	// calls to startLogProduction.
	go func(cancel context.CancelCauseFunc, done chan struct{}) {
		// Ensure the context is cancelled when log productions completes
		// so that GetLogProductionErrorChannel functions correctly.
		defer cancel(nil)
		// Signal that the goroutine has exited so stopLogProduction can drain.
		defer close(done)

		c.logProducer(stdout, stderr)
	}(c.logProductionCancel, c.logProductionDone)

	return nil
}

// logProducer read logs from the container and writes them to stdout, stderr until either:
//   - logProductionCtx is done
//   - A fatal error occurs
//   - No more logs are available
func (c *DockerContainer) logProducer(stdout, stderr io.Writer) {
	// Clean up idle client connections.
	defer c.provider.Close()

	// Setup the log options, start from the beginning.
	options := &client.ContainerLogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	}

	// Use a separate method so that timeout cancel function is
	// called correctly.
	for c.copyLogsTimeout(stdout, stderr, options) {
	}
}

// copyLogsTimeout copies logs from the container to stdout and stderr with a timeout.
// It returns true if the log production should be retried, false otherwise.
func (c *DockerContainer) copyLogsTimeout(stdout, stderr io.Writer, options *client.ContainerLogsOptions) bool {
	timeoutCtx, cancel := context.WithTimeout(c.logProductionCtx, *c.logProductionTimeout)
	defer cancel()

	err := c.copyLogs(timeoutCtx, stdout, stderr, *options)
	switch {
	case err == nil:
		// No more logs available.
		return false
	case c.logProductionCtx.Err() != nil:
		// Log production was stopped or caller context is done.
		return false
	case timeoutCtx.Err() != nil, errors.Is(err, net.ErrClosed):
		// Timeout or client connection closed, retry.
	default:
		// Unexpected error, retry.
		c.logger.Printf("Unexpected error reading logs: %v", err)
	}

	// Retry from the last log received.
	now := time.Now()
	options.Since = fmt.Sprintf("%d.%09d", now.Unix(), int64(now.Nanosecond()))

	return true
}

// copyLogs copies logs from the container to stdout and stderr.
func (c *DockerContainer) copyLogs(ctx context.Context, stdout, stderr io.Writer, options client.ContainerLogsOptions) error {
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

	// Wait for the log production goroutine to finish draining any buffered
	// logs before cancelling. When the container has already exited, the
	// goroutine will reach EOF naturally and close logProductionDone on its
	// own. The bounded timeout prevents blocking indefinitely when the
	// container is still actively streaming (e.g. Stop() called on a running
	// container).
	if c.logProductionDone != nil {
		select {
		case <-c.logProductionDone:
			// Goroutine already finished naturally; nothing more to do.
			return nil
		case <-time.After(minLogProductionTimeout):
			// Timed out waiting for natural exit; force-cancel now.
		}
	}

	// Signal the log production to stop (for still-running containers).
	c.logProductionCancel(errLogProductionStop)

	// Wait for the goroutine to acknowledge the cancellation. Context
	// cancellation propagates into the Docker transport and should unblock
	// stdcopy.StdCopy promptly, but we bound the wait to match
	// minLogProductionTimeout to guard against stuck kernel socket reads or
	// daemon transport failures that might not honour context cancellation.
	if c.logProductionDone != nil {
		select {
		case <-c.logProductionDone:
		case <-time.After(minLogProductionTimeout):
			c.logger.Printf("timeout waiting for log production goroutine to exit; a goroutine may have leaked")
		}
	}

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
	go func(ctx context.Context) {
		<-ctx.Done()
		errCh <- context.Cause(ctx)
		close(errCh)
	}(c.logProductionCtx)

	return errCh
}

// connectReaper connects the reaper to the container if it is needed.
func (c *DockerContainer) connectReaper(ctx context.Context) error {
	if c.provider.config.RyukDisabled || isReaperImage(c.Image) {
		// Reaper is disabled or we are the reaper container.
		return nil
	}

	reaper, err := spawner.reaper(context.WithValue(ctx, core.DockerHostContextKey, c.provider.host), core.SessionID(), c.provider)
	if err != nil {
		return fmt.Errorf("reaper: %w", err)
	}

	if c.terminationSignal, err = reaper.Connect(); err != nil {
		return fmt.Errorf("reaper connect: %w", err)
	}

	return nil
}

// cleanupTermSignal triggers the termination signal if it was created and an error occurred.
func (c *DockerContainer) cleanupTermSignal(err error) {
	if c.terminationSignal != nil && err != nil {
		c.terminationSignal <- true
	}
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

	_, err := n.provider.client.NetworkRemove(ctx, n.ID, client.NetworkRemoveOptions{})
	return err
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
	mtx       sync.Mutex
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
	var buildOptions client.ImageBuildOptions
	resp, err := backoff.RetryNotifyWithData(
		func() (client.ImageBuildResult, error) {
			var err error
			buildOptions, err = img.BuildOptions()
			if err != nil {
				return client.ImageBuildResult{}, backoff.Permanent(fmt.Errorf("build options: %w", err))
			}
			defer tryClose(buildOptions.Context) // release resources in any case

			resp, err := p.client.ImageBuild(ctx, buildOptions.Context, buildOptions)
			if err != nil {
				if isPermanentClientError(err) {
					return client.ImageBuildResult{}, backoff.Permanent(fmt.Errorf("build image: %w", err))
				}
				return client.ImageBuildResult{}, err
			}
			defer p.Close()

			return resp, nil
		},
		backoff.WithContext(backoff.NewExponentialBackOff(), ctx),
		func(err error, _ time.Duration) {
			p.Logger.Printf("Failed to build image: %s, will retry", err)
		},
	)
	if err != nil {
		return "", err // Error is already wrapped.
	}
	defer resp.Body.Close()

	// Always process the output, even if it is not printed
	// to ensure that errors during the build process are
	// correctly handled.
	if err = jsonmessage.DisplayStream(resp.Body, img.BuildLogWriter()); err != nil {
		return "", fmt.Errorf("build image: %w", err)
	}

	// the first tag is the one we want
	return buildOptions.Tags[0], nil
}

// CreateContainer fulfils a request for a container without starting it
func (p *DockerProvider) CreateContainer(ctx context.Context, req ContainerRequest) (con Container, err error) {
	// defer the close of the Docker client connection the soonest
	defer p.Close()

	var defaultNetwork string
	defaultNetwork, err = p.ensureDefaultNetwork(ctx)
	if err != nil {
		return nil, fmt.Errorf("ensure default network: %w", err)
	}

	// If default network is not bridge make sure it is attached to the request
	// as container won't be attached to it automatically
	// in case of Podman the bridge network is called 'podman' as 'bridge' would conflict
	if defaultNetwork != p.defaultBridgeNetworkName {
		isAttached := slices.Contains(req.Networks, defaultNetwork)

		if !isAttached {
			req.Networks = append(req.Networks, defaultNetwork)
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

	if err = req.Validate(); err != nil {
		return nil, err
	}

	// always append the hub substitutor after the user-defined ones
	req.ImageSubstitutors = append(req.ImageSubstitutors, newPrependHubRegistry(p.config.HubImageNamePrefix))

	var platform *specs.Platform

	defaultHooks := []ContainerLifecycleHooks{
		DefaultLoggingHook(p.Logger),
	}

	origLifecycleHooks := req.LifecycleHooks
	req.LifecycleHooks = []ContainerLifecycleHooks{
		combineContainerHooks(defaultHooks, req.LifecycleHooks),
	}

	if req.ShouldBuildImage() {
		if err = req.buildingHook(ctx); err != nil {
			return nil, err
		}

		imageName, err = p.BuildImage(ctx, &req)
		if err != nil {
			return nil, err
		}

		req.Image = imageName
		if err = req.builtHook(ctx); err != nil {
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
			img, err := p.client.ImageInspect(ctx, imageName)
			if err != nil {
				if !errdefs.IsNotFound(err) {
					return nil, err
				}
				shouldPullImage = true
			}
			if platform != nil && (img.Architecture != platform.Architecture || img.Os != platform.OS) {
				shouldPullImage = true
			}
		}

		if shouldPullImage {
			pullOpt := client.ImagePullOptions{}
			if req.ImagePlatform != "" {
				if pf, err := platforms.Parse(req.ImagePlatform); err == nil {
					pullOpt.Platforms = append(pullOpt.Platforms, pf)
				}
			}
			if err := p.attemptToPullImage(ctx, imageName, pullOpt); err != nil {
				return nil, err
			}
		}
	}

	if !isReaperImage(imageName) {
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
	}

	hostConfig := &container.HostConfig{
		Tmpfs: req.Tmpfs,
	}

	networkingConfig := &network.NetworkingConfig{}

	// default hooks include logger hook and pre-create hook
	defaultHooks = append(defaultHooks,
		defaultPreCreateHook(p, dockerInput, hostConfig, networkingConfig),
		defaultCopyFileToContainerHook(req.Files),
		defaultLogConsumersHook(req.LogConsumerCfg),
		defaultReadinessHook(),
	)

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

		defer func() {
			if err != nil && con == nil {
				// Container setup failed so ensure we clean up the sshd container too.
				ctr := &DockerContainer{
					provider:       p,
					logger:         p.Logger,
					lifecycleHooks: []ContainerLifecycleHooks{sshdForwardPortsHook},
				}
				err = errors.Join(ctr.terminatingHook(ctx))
			}
		}()

		defaultHooks = append(defaultHooks, sshdForwardPortsHook)
	}

	// Combine with the original LifecycleHooks to avoid duplicate logging hooks.
	req.LifecycleHooks = []ContainerLifecycleHooks{
		combineContainerHooks(defaultHooks, origLifecycleHooks),
	}

	err = req.creatingHook(ctx)
	if err != nil {
		return nil, err
	}

	resp, err := p.client.ContainerCreate(ctx, client.ContainerCreateOptions{
		Config:           dockerInput,
		HostConfig:       hostConfig,
		NetworkingConfig: networkingConfig,
		Platform:         platform,
		Name:             req.Name,
	})
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
				_, err = p.client.NetworkConnect(ctx, nw.ID, client.NetworkConnectOptions{
					Container:      resp.ID,
					EndpointConfig: &endpointSetting,
				})
				if err != nil {
					return nil, fmt.Errorf("network connect: %w", err)
				}
			}
		}
	}

	// This should match the fields set in ContainerFromDockerResponse.
	ctr := &DockerContainer{
		ID:             resp.ID,
		WaitingFor:     req.WaitingFor,
		Image:          imageName,
		imageWasBuilt:  req.ShouldBuildImage(),
		keepBuiltImage: req.ShouldKeepBuiltImage(),
		sessionID:      req.sessionID(),
		exposedPorts:   req.ExposedPorts,
		provider:       p,
		logger:         p.Logger,
		lifecycleHooks: req.LifecycleHooks,
	}

	if err = ctr.connectReaper(ctx); err != nil {
		return ctr, err // No wrap as it would stutter.
	}

	// Wrapped so the returned error is passed to the cleanup function.
	defer func(ctr *DockerContainer) {
		ctr.cleanupTermSignal(err)
	}(ctr)

	if err = ctr.createdHook(ctx); err != nil {
		// Return the container to allow caller to clean up.
		return ctr, fmt.Errorf("created hook: %w", err)
	}

	return ctr, nil
}

func (p *DockerProvider) findContainerByName(ctx context.Context, name string) (*container.Summary, error) {
	if name == "" {
		return nil, nil
	}

	// Note that, 'name' filter will use regex to find the containers
	containers, err := p.client.ContainerList(ctx, client.ContainerListOptions{
		All:     true,
		Filters: make(client.Filters).Add("name", fmt.Sprintf("^%s$", name)),
	})
	if err != nil {
		return nil, fmt.Errorf("container list: %w", err)
	}
	defer p.Close()

	if len(containers.Items) > 0 {
		return &containers.Items[0], nil
	}
	return nil, nil
}

func (p *DockerProvider) waitContainerCreation(ctx context.Context, name string) (*container.Summary, error) {
	return backoff.RetryNotifyWithData(
		func() (*container.Summary, error) {
			c, err := p.findContainerByName(ctx, name)
			if err != nil {
				if !errdefs.IsNotFound(err) && isPermanentClientError(err) {
					return nil, backoff.Permanent(err)
				}
				return nil, err
			}

			if c == nil {
				return nil, errdefs.ErrNotFound.WithMessage(fmt.Sprintf("container %s not found", name))
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

func (p *DockerProvider) ReuseOrCreateContainer(ctx context.Context, req ContainerRequest) (con Container, err error) {
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

	sessionID := req.sessionID()

	var termSignal chan bool
	if !p.config.RyukDisabled {
		r, err := spawner.reaper(context.WithValue(ctx, core.DockerHostContextKey, p.host), sessionID, p)
		if err != nil {
			return nil, fmt.Errorf("reaper: %w", err)
		}

		termSignal, err = r.Connect()
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

	// Workaround for https://github.com/moby/moby/issues/50133.
	// /containers/{id}/json API endpoint of Docker Engine takes data about container from master (not replica) database
	// which is synchronized with container state after call of /containers/{id}/stop API endpoint.
	dcState, err := dc.State(ctx)
	if err != nil {
		return nil, fmt.Errorf("docker container state: %w", err)
	}

	// If a container was stopped programmatically, we want to ensure the container
	// is running again, but only if it is not paused, as it's not possible to start
	// a paused container. The Docker Engine returns the "cannot start a paused container,
	// try unpause instead" error.
	switch dcState.Status {
	case container.StateRunning:
		// cannot re-start a running container, but we still need
		// to call the startup hooks.
	case container.StatePaused:
		// TODO: we should unpause the container here.
		return nil, fmt.Errorf("cannot start a paused container: %w", errors.ErrUnsupported)
	default:
		if err := dc.Start(ctx); err != nil {
			return dc, fmt.Errorf("start container %s in state %s: %w", req.Name, c.State, err)
		}
	}

	err = dc.startedHook(ctx)
	if err != nil {
		return nil, err
	}

	dc.isRunning.Store(true)

	err = dc.readiedHook(ctx)
	if err != nil {
		return nil, err
	}

	return dc, nil
}

// attemptToPullImage tries to pull the image while respecting the ctx cancellations.
// Besides, if the image cannot be pulled due to ErrorNotFound then no need to retry but terminate immediately.
func (p *DockerProvider) attemptToPullImage(ctx context.Context, tag string, pullOpt client.ImagePullOptions) error {
	registry, imageAuth, err := DockerImageAuth(ctx, tag)
	if err != nil {
		p.Logger.Printf("No image auth found for %s. Setting empty credentials for the image: %s. This is expected for public images. Details: %s", registry, tag, err)
	} else {
		// see https://github.com/docker/docs/blob/e8e1204f914767128814dca0ea008644709c117f/engine/api/sdk/examples.md?plain=1#L649-L657
		if encodedAuth, err := authconfig.Encode(imageAuth); err != nil {
			p.Logger.Printf("Failed to marshal image auth. Setting empty credentials for the image: %s. Error is: %s", tag, err)
		} else {
			pullOpt.RegistryAuth = encodedAuth
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
		func(err error, _ time.Duration) {
			p.Logger.Printf("Failed to pull image: %s, will retry", err)
		},
	)
	if err != nil {
		return err
	}
	defer pull.Close()

	// download of docker image finishes at EOF of the pull request
	_, err = io.Copy(io.Discard, pull)
	return err
}

// Health measure the healthiness of the provider. Right now we leverage the
// docker-client Info endpoint to see if the daemon is reachable.
func (p *DockerProvider) Health(ctx context.Context) error {
	_, err := p.client.Info(ctx, client.InfoOptions{})
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
	p.mtx.Lock()
	defer p.mtx.Unlock()

	return p.daemonHostLocked(ctx)
}

func (p *DockerProvider) daemonHostLocked(ctx context.Context) (string, error) {
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
			defaultNetwork, err := p.ensureDefaultNetworkLocked(ctx)
			if err != nil {
				return "", fmt.Errorf("ensure default network: %w", err)
			}
			ip, err := p.getGatewayIP(ctx, defaultNetwork)
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
func (p *DockerProvider) CreateNetwork(ctx context.Context, req NetworkRequest) (net Network, err error) {
	// defer the close of the Docker client connection the soonest
	defer p.Close()

	if _, err = p.ensureDefaultNetwork(ctx); err != nil {
		return nil, fmt.Errorf("ensure default network: %w", err)
	}

	if req.Labels == nil {
		req.Labels = make(map[string]string)
	}

	nc := client.NetworkCreateOptions{
		Driver:     req.Driver,
		Internal:   req.Internal,
		EnableIPv6: req.EnableIPv6,
		Attachable: req.Attachable,
		Labels:     req.Labels,
		IPAM:       req.IPAM,
	}

	sessionID := req.sessionID()

	var termSignal chan bool
	if !p.config.RyukDisabled {
		r, err := spawner.reaper(context.WithValue(ctx, core.DockerHostContextKey, p.host), sessionID, p)
		if err != nil {
			return nil, fmt.Errorf("reaper: %w", err)
		}

		termSignal, err = r.Connect()
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
	networkResource, err := p.client.NetworkInspect(ctx, req.Name, client.NetworkInspectOptions{
		Verbose: true,
	})
	if err != nil {
		return network.Inspect{}, err
	}

	return networkResource.Network, err
}

func (p *DockerProvider) GetGatewayIP(ctx context.Context) (string, error) {
	// Use a default network as defined in the DockerProvider
	defaultNetwork, err := p.ensureDefaultNetwork(ctx)
	if err != nil {
		return "", fmt.Errorf("ensure default network: %w", err)
	}
	return p.getGatewayIP(ctx, defaultNetwork)
}

func (p *DockerProvider) getGatewayIP(ctx context.Context, defaultNetwork string) (string, error) {
	nw, err := p.GetNetwork(ctx, NetworkRequest{Name: defaultNetwork})
	if err != nil {
		return "", err
	}

	var ip string
	for _, cfg := range nw.IPAM.Config {
		if cfg.Gateway.IsValid() {
			ip = cfg.Gateway.String()
			break
		}
	}
	if ip == "" {
		return "", errors.New("failed to get gateway IP from network settings")
	}

	return ip, nil
}

// ensureDefaultNetwork ensures that defaultNetwork is set and creates
// it if it does not exist, returning its value.
// It is safe to call this method concurrently.
func (p *DockerProvider) ensureDefaultNetwork(ctx context.Context) (string, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	return p.ensureDefaultNetworkLocked(ctx)
}

func (p *DockerProvider) ensureDefaultNetworkLocked(ctx context.Context) (string, error) {
	if p.defaultNetwork != "" {
		// Already set.
		return p.defaultNetwork, nil
	}

	networkResources, err := p.client.NetworkList(ctx, client.NetworkListOptions{})
	if err != nil {
		return "", fmt.Errorf("network list: %w", err)
	}

	// TODO: remove once we have docker context support via #2810
	// Prefer the default bridge network if it exists.
	// This makes the results stable as network list order is not guaranteed.
	for _, nw := range networkResources.Items {
		switch nw.Name {
		case p.defaultBridgeNetworkName:
			p.defaultNetwork = p.defaultBridgeNetworkName
			return p.defaultNetwork, nil
		case ReaperDefault:
			p.defaultNetwork = ReaperDefault
		}
	}

	if p.defaultNetwork != "" {
		return p.defaultNetwork, nil
	}

	// Create a bridge network for the container communications.
	_, err = p.client.NetworkCreate(ctx, ReaperDefault, client.NetworkCreateOptions{
		Driver:     Bridge,
		Attachable: true,
		Labels:     GenericLabels(),
	})
	// If the network already exists, we can ignore the error as that can
	// happen if we are running multiple tests in parallel and we only
	// need to ensure that the network exists.
	if err != nil && !errdefs.IsConflict(err) {
		return "", fmt.Errorf("network create: %w", err)
	}

	p.defaultNetwork = ReaperDefault

	return p.defaultNetwork, nil
}

// ContainerFromType builds a Docker container struct from the response of the Docker API
func (p *DockerProvider) ContainerFromType(ctx context.Context, response container.Summary) (ctr *DockerContainer, err error) {
	exposedPorts := make([]string, len(response.Ports))
	for i, port := range response.Ports {
		exposedPorts[i] = fmt.Sprintf("%d/%s", port.PublicPort, port.Type)
	}

	// This should match the fields set in CreateContainer.
	ctr = &DockerContainer{
		ID:            response.ID,
		Image:         response.Image,
		imageWasBuilt: false,
		sessionID:     response.Labels[core.LabelSessionID],
		exposedPorts:  exposedPorts,
		provider:      p,
		logger:        p.Logger,
		lifecycleHooks: []ContainerLifecycleHooks{
			DefaultLoggingHook(p.Logger),
		},
	}
	ctr.isRunning.Store(response.State == "running")

	if err = ctr.connectReaper(ctx); err != nil {
		return nil, err
	}

	// Wrapped so the returned error is passed to the cleanup function.
	defer func(ctr *DockerContainer) {
		ctr.cleanupTermSignal(err)
	}(ctr)

	// populate the raw representation of the container
	resp, err := ctr.inspectRawContainer(ctx)
	if err != nil {
		// Return the container to allow caller to clean up.
		return ctr, fmt.Errorf("inspect raw container: %w", err)
	}

	// the health status of the container, if any
	if health := resp.Container.State.Health; health != nil {
		ctr.healthStatus = health.Status
	}

	return ctr, nil
}

// ListImages list images from the provider. If an image has multiple Tags, each tag is reported
// individually with the same ID and same labels
func (p *DockerProvider) ListImages(ctx context.Context) ([]ImageInfo, error) {
	images := []ImageInfo{}

	imageList, err := p.client.ImageList(ctx, client.ImageListOptions{})
	if err != nil {
		return images, fmt.Errorf("listing images %w", err)
	}

	for _, img := range imageList.Items {
		for _, tag := range img.RepoTags {
			images = append(images, ImageInfo{ID: img.ID, Name: tag})
		}
	}

	return images, nil
}

// SaveImages exports a list of images as an uncompressed tar
func (p *DockerProvider) SaveImages(ctx context.Context, output string, images ...string) error {
	return p.SaveImagesWithOpts(ctx, output, images)
}

// SaveImagesWithOpts exports a list of images as an uncompressed tar, passing options to the provider
func (p *DockerProvider) SaveImagesWithOpts(ctx context.Context, output string, images []string, opts ...SaveImageOption) error {
	saveOpts := saveImageOptions{}

	for _, opt := range opts {
		if err := opt(&saveOpts); err != nil {
			return fmt.Errorf("applying save image option: %w", err)
		}
	}

	outputFile, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("opening output file %w", err)
	}
	defer func() {
		_ = outputFile.Close()
	}()

	imageReader, err := p.client.ImageSave(ctx, images, saveOpts.dockerSaveOpts...)
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

func SaveDockerImageWithPlatforms(platforms ...specs.Platform) SaveImageOption {
	return func(opts *saveImageOptions) error {
		opts.dockerSaveOpts = append(opts.dockerSaveOpts, client.ImageSaveWithPlatforms(platforms...))
		opts.platforms = append(opts.platforms, platforms...)

		return nil
	}
}

// PullImage pulls image from registry
func (p *DockerProvider) PullImage(ctx context.Context, img string) error {
	return p.attemptToPullImage(ctx, img, client.ImagePullOptions{})
}

// PullImageWithOpts pulls image from registry, passing options to the provider.
func (p *DockerProvider) PullImageWithOpts(ctx context.Context, img string, opts ...PullImageOption) error {
	pullOpts := pullImageOptions{}

	for _, opt := range opts {
		if err := opt(&pullOpts); err != nil {
			return fmt.Errorf("applying pull image option: %w", err)
		}
	}

	return p.attemptToPullImage(ctx, img, pullOpts.dockerPullOpts)
}

func PullDockerImageWithPlatform(platform specs.Platform) PullImageOption {
	return func(opts *pullImageOptions) error {
		opts.dockerPullOpts.Platforms = append(opts.dockerPullOpts.Platforms, platform)

		return nil
	}
}

var permanentClientErrors = []func(error) bool{
	errdefs.IsNotFound,
	errdefs.IsInvalidArgument,
	errdefs.IsUnauthorized,
	errdefs.IsPermissionDenied,
	errdefs.IsNotImplemented,
	errdefs.IsInternal,
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
