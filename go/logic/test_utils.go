package logic

import (
	"context"
	"fmt"

	"github.com/github/gh-ost/go/mysql"
	"github.com/testcontainers/testcontainers-go"
)

func GetConnectionConfig(ctx context.Context, container testcontainers.Container) (*mysql.ConnectionConfig, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return nil, err
	}

	port, err := container.MappedPort(ctx, "3306")
	if err != nil {
		return nil, err
	}

	connectionConfig := mysql.NewConnectionConfig()
	connectionConfig.Key.Hostname = host
	connectionConfig.Key.Port = port.Int()
	connectionConfig.User = "root"
	connectionConfig.Password = "root-password"

	return connectionConfig, nil
}

func GetDSN(ctx context.Context, container testcontainers.Container) (string, error) {
	host, err := container.Host(ctx)
	if err != nil {
		return "", err
	}

	port, err := container.MappedPort(ctx, "3306")
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("root:root-password@tcp(%s:%s)/", host, port.Port()), nil
}
