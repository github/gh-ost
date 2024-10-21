package exec

import (
	"bytes"
	"io"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/stdcopy"
)

// ProcessOptions defines options applicable to the reader processor
type ProcessOptions struct {
	ExecConfig container.ExecOptions
	Reader     io.Reader
}

// NewProcessOptions returns a new ProcessOptions instance
// with the given command and default options:
// - detach: false
// - attach stdout: true
// - attach stderr: true
func NewProcessOptions(cmd []string) *ProcessOptions {
	return &ProcessOptions{
		ExecConfig: container.ExecOptions{
			Cmd:          cmd,
			Detach:       false,
			AttachStdout: true,
			AttachStderr: true,
		},
	}
}

// ProcessOption defines a common interface to modify the reader processor
// These options can be passed to the Exec function in a variadic way to customize the returned Reader instance
type ProcessOption interface {
	Apply(opts *ProcessOptions)
}

type ProcessOptionFunc func(opts *ProcessOptions)

func (fn ProcessOptionFunc) Apply(opts *ProcessOptions) {
	fn(opts)
}

func WithUser(user string) ProcessOption {
	return ProcessOptionFunc(func(opts *ProcessOptions) {
		opts.ExecConfig.User = user
	})
}

func WithWorkingDir(workingDir string) ProcessOption {
	return ProcessOptionFunc(func(opts *ProcessOptions) {
		opts.ExecConfig.WorkingDir = workingDir
	})
}

func WithEnv(env []string) ProcessOption {
	return ProcessOptionFunc(func(opts *ProcessOptions) {
		opts.ExecConfig.Env = env
	})
}

// Multiplexed returns a [ProcessOption] that configures the command execution
// to combine stdout and stderr into a single stream without Docker's multiplexing headers.
func Multiplexed() ProcessOption {
	return ProcessOptionFunc(func(opts *ProcessOptions) {
		// returning fast to bypass those options with a nil reader,
		// which could be the case when other options are used
		// to configure the exec creation.
		if opts.Reader == nil {
			return
		}

		done := make(chan struct{})

		var outBuff bytes.Buffer
		var errBuff bytes.Buffer
		go func() {
			if _, err := stdcopy.StdCopy(&outBuff, &errBuff, opts.Reader); err != nil {
				return
			}
			close(done)
		}()

		<-done

		opts.Reader = io.MultiReader(&outBuff, &errBuff)
	})
}
