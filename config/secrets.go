package config

import (
	"context"
	"fmt"
	"os"

	vault "github.com/hashicorp/vault/api"
)

type Client struct {
	client *vault.Client
}

func readVaultSecrets(ctx context.Context) (map[string]any, error) {
	client, err := NewVaultClient()
	if err != nil {
		return nil, err
	}
	return client.GetBackendSecrets(ctx)
}

func NewVaultClient() (*Client, error) {
	cfg := vault.DefaultConfig()

	if err := cfg.ReadEnvironment(); err != nil {
		return nil, fmt.Errorf("read vault environment: %w", err)
	}

	client, err := vault.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create vault client: %w", err)
	}

	client.SetToken(os.Getenv("VAULT_TOKEN"))

	return &Client{
		client: client,
	}, nil
}

func (c *Client) GetBackendSecrets(ctx context.Context) (map[string]any, error) {
	secret, err := c.client.KVv2("app").Get(ctx, "job-manager")
	if err != nil {
		return nil, fmt.Errorf("read backend secrets: %w", err)
	}

	return secret.Data, nil
}
