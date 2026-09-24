package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadEnvOptionalAndPreservesEnvironment(t *testing.T) {
	cleanEnvironment(t)
	t.Chdir(t.TempDir())
	if err := LoadEnv(); err != nil {
		t.Fatalf("missing .env: %v", err)
	}
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("DB_MIN_CONNS", "")
	if err := os.Unsetenv("DB_URL"); err != nil {
		t.Fatal(err)
	}
	contents := "HTTP_ADDR=:8080\nDB_MIN_CONNS=1\nDB_MAX_CONNS=20\nDB_URL=" + testTemplate + "\n"
	if err := os.WriteFile(".env", []byte(contents), 0600); err != nil {
		t.Fatal(err)
	}
	if err := LoadEnv(); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("HTTP_ADDR") != ":9090" || os.Getenv("DB_MIN_CONNS") != "" || os.Getenv("DB_MAX_CONNS") != "20" || os.Getenv("DB_URL") != testTemplate {
		t.Fatal("dotenv did not preserve environment precedence or URL placeholders")
	}
}

func TestLoadEnvRejectsMalformedOrUnreadableFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	if err := os.WriteFile(path, []byte("INVALID!KEY=secret-marker\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{path, dir} {
		err := loadEnvFile(path)
		if err == nil || strings.Contains(err.Error(), "secret-marker") {
			t.Fatalf("expected safe dotenv error, got %v", err)
		}
	}
}
