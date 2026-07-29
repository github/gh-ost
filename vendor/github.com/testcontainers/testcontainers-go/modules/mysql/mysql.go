package mysql

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	rootUser            = "root"
	defaultUser         = "test"
	defaultPassword     = "test"
	defaultDatabaseName = "test"
)

// MySQLContainer represents the MySQL container type used in the module
type MySQLContainer struct {
	testcontainers.Container
	username string
	password string
	database string
}

func WithDefaultCredentials() testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		username := req.Env["MYSQL_USER"]
		password := req.Env["MYSQL_PASSWORD"]
		if strings.EqualFold(rootUser, username) {
			delete(req.Env, "MYSQL_USER")
		}
		if len(password) != 0 && password != "" {
			req.Env["MYSQL_ROOT_PASSWORD"] = password
		} else if strings.EqualFold(rootUser, username) {
			req.Env["MYSQL_ALLOW_EMPTY_PASSWORD"] = "yes"
			delete(req.Env, "MYSQL_PASSWORD")
		}

		return nil
	}
}

// Deprecated: use Run instead
// RunContainer creates an instance of the MySQL container type
func RunContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*MySQLContainer, error) {
	return Run(ctx, "mysql:8.0.36", opts...)
}

// Run creates an instance of the MySQL container type
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*MySQLContainer, error) {
	moduleOpts := []testcontainers.ContainerCustomizer{
		testcontainers.WithExposedPorts("3306/tcp", "33060/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MYSQL_USER":     defaultUser,
			"MYSQL_PASSWORD": defaultPassword,
			"MYSQL_DATABASE": defaultDatabaseName,
		}),
		testcontainers.WithWaitStrategy(wait.ForLog("port: 3306  MySQL Community Server")),
	}

	moduleOpts = append(moduleOpts, opts...)
	moduleOpts = append(moduleOpts, WithDefaultCredentials())

	// Validate credentials after applying all options
	validateCreds := func(req *testcontainers.GenericContainerRequest) error {
		username, ok := req.Env["MYSQL_USER"]
		if !ok {
			username = rootUser
		}
		password := req.Env["MYSQL_PASSWORD"]

		if len(password) == 0 && password == "" && !strings.EqualFold(rootUser, username) {
			return errors.New("empty password can be used only with the root user")
		}
		return nil
	}

	moduleOpts = append(moduleOpts, testcontainers.CustomizeRequestOption(validateCreds))

	ctr, err := testcontainers.Run(ctx, img, moduleOpts...)
	var c *MySQLContainer
	if ctr != nil {
		c = &MySQLContainer{
			Container: ctr,
			username:  rootUser, // default to root, will be overridden if MYSQL_USER is set
		}
	}

	if err != nil {
		return c, fmt.Errorf("run mysql: %w", err)
	}

	// Retrieve credentials from container environment
	inspect, err := ctr.Inspect(ctx)
	if err != nil {
		return c, fmt.Errorf("inspect mysql: %w", err)
	}

	var foundUser, foundPass, foundDB bool
	for _, env := range inspect.Config.Env {
		if v, ok := strings.CutPrefix(env, "MYSQL_USER="); ok {
			c.username, foundUser = v, true
		}
		if v, ok := strings.CutPrefix(env, "MYSQL_PASSWORD="); ok {
			c.password, foundPass = v, true
		}
		if v, ok := strings.CutPrefix(env, "MYSQL_DATABASE="); ok {
			c.database, foundDB = v, true
		}

		if foundUser && foundPass && foundDB {
			break
		}
	}

	return c, nil
}

// MustConnectionString panics if the address cannot be determined.
func (c *MySQLContainer) MustConnectionString(ctx context.Context, args ...string) string {
	addr, err := c.ConnectionString(ctx, args...)
	if err != nil {
		panic(err)
	}
	return addr
}

func (c *MySQLContainer) ConnectionString(ctx context.Context, args ...string) (string, error) {
	endpoint, err := c.PortEndpoint(ctx, "3306/tcp", "")
	if err != nil {
		return "", err
	}

	extraArgs := ""
	if len(args) > 0 {
		extraArgs = strings.Join(args, "&")
	}
	if extraArgs != "" {
		extraArgs = "?" + extraArgs
	}

	connectionString := fmt.Sprintf("%s:%s@tcp(%s)/%s%s", c.username, c.password, endpoint, c.database, extraArgs)
	return connectionString, nil
}

func WithUsername(username string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["MYSQL_USER"] = username

		return nil
	}
}

func WithPassword(password string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["MYSQL_PASSWORD"] = password

		return nil
	}
}

func WithDatabase(database string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["MYSQL_DATABASE"] = database

		return nil
	}
}

func WithConfigFile(configFile string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		cf := testcontainers.ContainerFile{
			HostFilePath:      configFile,
			ContainerFilePath: "/etc/mysql/conf.d/my.cnf",
			FileMode:          0o755,
		}
		req.Files = append(req.Files, cf)

		return nil
	}
}

func WithScripts(scripts ...string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		initScripts := make([]testcontainers.ContainerFile, 0, len(scripts))
		for _, script := range scripts {
			cf := testcontainers.ContainerFile{
				HostFilePath:      script,
				ContainerFilePath: "/docker-entrypoint-initdb.d/" + filepath.Base(script),
				FileMode:          0o755,
			}
			initScripts = append(initScripts, cf)
		}
		req.Files = append(req.Files, initScripts...)

		return nil
	}
}
