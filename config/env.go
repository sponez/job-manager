package config

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
)

// LoadEnv loads an optional .env from the working directory. Existing environment
// variables, including explicitly empty ones, take precedence.
func LoadEnv() error {
	return loadEnvFile(".env")
}

func loadEnvFile(path string) error {
	if err := godotenv.Load(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		// dotenv parse errors can include the input, which may contain secrets.
		return errors.New("cannot load .env: check file permissions and dotenv syntax")
	}
	return nil
}
