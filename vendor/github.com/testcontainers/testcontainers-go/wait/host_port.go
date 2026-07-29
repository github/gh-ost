package wait

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/moby/moby/api/types/network"

	"github.com/testcontainers/testcontainers-go/log"
)

const (
	exitEaccess     = 126 // container cmd can't be invoked (permission denied)
	exitCmdNotFound = 127 // container cmd not found/does not exist or invalid bind-mount
)

// Implement interface
var (
	_ Strategy        = (*HostPortStrategy)(nil)
	_ StrategyTimeout = (*HostPortStrategy)(nil)
)

var (
	errShellNotExecutable = errors.New("/bin/sh command not executable")
	errShellNotFound      = errors.New("/bin/sh command not found")
)

type HostPortStrategy struct {
	// Port is a string containing port number and protocol in the format "80/tcp"
	// which
	Port string
	// all WaitStrategies should have a startupTimeout to avoid waiting infinitely
	timeout      *time.Duration
	PollInterval time.Duration

	// skipInternalCheck is a flag to skip the internal check, which is useful when
	// a shell is not available in the container or when the container doesn't bind
	// the port internally until additional conditions are met.
	skipInternalCheck bool

	// skipExternalCheck is a flag to skip the external check, which, if used with
	// skipInternalCheck, makes strategy waiting only for port mapping completion
	// without accessing port.
	skipExternalCheck bool
}

// NewHostPortStrategy constructs a default host port strategy that waits for the given
// port to be exposed. The default startup timeout is 60 seconds.
func NewHostPortStrategy(port string) *HostPortStrategy {
	return &HostPortStrategy{
		Port:         port,
		PollInterval: defaultPollInterval(),
	}
}

// fluent builders for each property
// since go has neither covariance nor generics, the return type must be the type of the concrete implementation
// this is true for all properties, even the "shared" ones like startupTimeout

// ForListeningPort returns a host port strategy that waits for the given port
// to be exposed and bound internally the container.
// Alias for `NewHostPortStrategy(port)`.
func ForListeningPort(port string) *HostPortStrategy {
	return NewHostPortStrategy(port)
}

// ForExposedPort returns a host port strategy that waits for the first port
// to be exposed and bound internally the container.
func ForExposedPort() *HostPortStrategy {
	return NewHostPortStrategy("")
}

// ForMappedPort returns a host port strategy that waits for the given port
// to be mapped without accessing the port itself.
func ForMappedPort(port string) *HostPortStrategy {
	return NewHostPortStrategy(port).SkipInternalCheck().SkipExternalCheck()
}

// SkipInternalCheck changes the host port strategy to skip the internal check,
// which is useful when a shell is not available in the container or when the
// container doesn't bind the port internally until additional conditions are met.
func (hp *HostPortStrategy) SkipInternalCheck() *HostPortStrategy {
	hp.skipInternalCheck = true

	return hp
}

// SkipExternalCheck changes the host port strategy to skip the external check,
// which, if used with SkipInternalCheck, makes strategy waiting only for port
// mapping completion without accessing port.
func (hp *HostPortStrategy) SkipExternalCheck() *HostPortStrategy {
	hp.skipExternalCheck = true

	return hp
}

// WithStartupTimeout can be used to change the default startup timeout
func (hp *HostPortStrategy) WithStartupTimeout(startupTimeout time.Duration) *HostPortStrategy {
	hp.timeout = &startupTimeout
	return hp
}

// WithPollInterval can be used to override the default polling interval of 100 milliseconds
func (hp *HostPortStrategy) WithPollInterval(pollInterval time.Duration) *HostPortStrategy {
	hp.PollInterval = pollInterval
	return hp
}

func (hp *HostPortStrategy) Timeout() *time.Duration {
	return hp.timeout
}

// String returns a human-readable description of the wait strategy.
func (hp *HostPortStrategy) String() string {
	port := "first exposed port"
	if hp.Port != "" {
		port = "port " + hp.Port
	}

	var checks string
	switch {
	case hp.skipInternalCheck && hp.skipExternalCheck:
		checks = " to be mapped"
	case hp.skipInternalCheck:
		checks = " to be accessible externally"
	case hp.skipExternalCheck:
		checks = " to be listening internally"
	default:
		checks = " to be listening"
	}

	return fmt.Sprintf("%s%s", port, checks)
}

// detectInternalPort returns the lowest internal port that is currently bound.
// If no internal port is found, it returns the zero nat.Port value which
// can be checked against an empty string.
func (hp *HostPortStrategy) detectInternalPort(ctx context.Context, target StrategyTarget) (network.Port, error) {
	var internalPort network.Port
	inspect, err := target.Inspect(ctx)
	if err != nil {
		return internalPort, fmt.Errorf("inspect: %w", err)
	}

	for port := range inspect.NetworkSettings.Ports {
		if internalPort.IsZero() || port.Num() < internalPort.Num() {
			internalPort = port
		}
	}

	return internalPort, nil
}

// WaitUntilReady implements Strategy.WaitUntilReady
func (hp *HostPortStrategy) WaitUntilReady(ctx context.Context, target StrategyTarget) error {
	timeout := defaultStartupTimeout()
	if hp.timeout != nil {
		timeout = *hp.timeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	waitInterval := hp.PollInterval

	var internalPort network.Port
	if hp.Port != "" {
		p, err := network.ParsePort(hp.Port)
		if err != nil {
			return err
		}
		internalPort = p
	}

	i := 0
	if internalPort.IsZero() {
		var err error
		// Port is not specified, so we need to detect it.
		internalPort, err = hp.detectInternalPort(ctx, target)
		if err != nil {
			return fmt.Errorf("detect internal port: %w", err)
		}

		for internalPort.IsZero() {
			select {
			case <-ctx.Done():
				return fmt.Errorf("detect internal port: retries: %d, last err: %w, ctx err: %w", i, err, ctx.Err())
			case <-time.After(waitInterval):
				if err := checkTarget(ctx, target); err != nil {
					return fmt.Errorf("detect internal port: check target: retries: %d, last err: %w", i, err)
				}

				internalPort, err = hp.detectInternalPort(ctx, target)
				if err != nil {
					return fmt.Errorf("detect internal port: %w", err)
				}
			}
		}
	}

	port, err := target.MappedPort(ctx, internalPort.String())
	i = 0

	for port.IsZero() {
		i++

		select {
		case <-ctx.Done():
			return fmt.Errorf("mapped port: retries: %d, port: %q, last err: %w, ctx err: %w", i, port, err, ctx.Err())
		case <-time.After(waitInterval):
			if err := checkTarget(ctx, target); err != nil {
				return fmt.Errorf("mapped port: check target: retries: %d, port: %q, last err: %w", i, port, err)
			}
			port, err = target.MappedPort(ctx, internalPort.String())
			if err != nil {
				log.Printf("mapped port: retries: %d, port: %q, err: %s\n", i, port, err)
			}
		}
	}

	if !hp.skipExternalCheck {
		ipAddress, err := target.Host(ctx)
		if err != nil {
			return fmt.Errorf("host: %w", err)
		}

		if err := externalCheck(ctx, ipAddress, port, target, waitInterval); err != nil {
			return fmt.Errorf("external check: %w", err)
		}
	}

	if hp.skipInternalCheck {
		return nil
	}

	if err = internalCheck(ctx, internalPort, target); err != nil {
		switch {
		case errors.Is(err, errShellNotExecutable):
			log.Printf("Shell not executable in container, only external port validated")
			return nil
		case errors.Is(err, errShellNotFound):
			log.Printf("Shell not found in container")
			return nil
		default:
			return fmt.Errorf("internal check: %w", err)
		}
	}

	return nil
}

func externalCheck(ctx context.Context, ipAddress string, port network.Port, target StrategyTarget, waitInterval time.Duration) error {
	proto := port.Proto()

	dialer := net.Dialer{}
	address := net.JoinHostPort(ipAddress, port.Port())
	for i := 0; ; i++ {
		if err := checkTarget(ctx, target); err != nil {
			return fmt.Errorf("check target: retries: %d address: %s: %w", i, address, err)
		}
		conn, err := dialer.DialContext(ctx, string(proto), address)
		if err != nil {
			var v *net.OpError
			if errors.As(err, &v) {
				var v2 *os.SyscallError
				if errors.As(v.Err, &v2) {
					if isConnRefusedErr(v2.Err) {
						time.Sleep(waitInterval)
						continue
					}
				}
			}
			return fmt.Errorf("dial: %w", err)
		}

		_ = conn.Close()
		return nil
	}
}

func internalCheck(ctx context.Context, internalPort network.Port, target StrategyTarget) error {
	command := buildInternalCheckCommand(internalPort.Num())
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := checkTarget(ctx, target); err != nil {
			return err
		}
		exitCode, _, err := target.Exec(ctx, []string{"/bin/sh", "-c", command})
		if err != nil {
			return fmt.Errorf("%w, host port waiting failed", err)
		}

		// Docker has an issue which override exit code 127 to 126 due to:
		// https://github.com/moby/moby/issues/45795
		// Handle both to ensure compatibility with Docker and Podman for now.
		switch exitCode {
		case 0:
			return nil
		case exitEaccess:
			return errShellNotExecutable
		case exitCmdNotFound:
			return errShellNotFound
		}
	}
}

func buildInternalCheckCommand(internalPort uint16) string {
	command := `(
					cat /proc/net/tcp* | awk '{print $2}' | grep -i :%04x ||
					nc -vz -w 1 localhost %d ||
					/bin/sh -c '</dev/tcp/localhost/%d'
				)
				`
	return "true && " + fmt.Sprintf(command, internalPort, internalPort, internalPort)
}
