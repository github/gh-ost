package logic

import (
	"context"
	"fmt"

	"github.com/github/gh-ost/go/mysql"
	"github.com/testcontainers/testcontainers-go"
)

func GetDSN(ctx context.Context, container testcontainers.Container) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("root:root-password@tcp(%s:%s)/", host, port.Port()), nil
}

func GetConnectionConfig(ctx context.Context, container testcontainers.Container) (*mysql.ConnectionConfig, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := container.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return nil, err
	}
	config := mysql.NewConnectionConfig()
	config.Key.Hostname = host
	config.Key.Port = port.Int()
	config.User = "root"
	config.Password = "root-password"
	return config, nil
}
