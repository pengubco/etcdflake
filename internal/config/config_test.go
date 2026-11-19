package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const minimal = `{"etcd": {"endpoints": ["127.0.0.1:2379"], "prefix": "/i3d"}}`

func TestDefaults(t *testing.T) {
	c, err := Parse([]byte(minimal))
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:7669", c.GRPCAddr)
	assert.Equal(t, "127.0.0.1:8001", c.HTTPAddr)
	assert.Empty(t, c.DebugAddr)
	assert.Equal(t, "info", c.LogLevel)
	assert.Equal(t, 1024, c.MaxWorkers)
	assert.Equal(t, "/i3d/", c.Etcd.Prefix)
}

func TestPasswordFromEnv(t *testing.T) {
	t.Setenv(PasswordEnv, "s3cret")
	c, err := Parse([]byte(`{"etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/", "username": "i3d", "password_file": "/nonexistent"}}`))
	require.NoError(t, err)
	assert.Equal(t, "s3cret", c.Etcd.Password, "env wins over file")
}

func TestPasswordFromFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "pw")
	require.NoError(t, os.WriteFile(f, []byte("fromfile\n"), 0o600))
	c, err := Parse([]byte(`{"etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/", "username": "i3d", "password_file": "` + f + `"}}`))
	require.NoError(t, err)
	assert.Equal(t, "fromfile", c.Etcd.Password)
}

func TestRejects(t *testing.T) {
	for name, js := range map[string]string{
		"unknown field (e.g. inline password)": `{"etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/", "password": "x"}}`,
		"no endpoints":                         `{"etcd": {"prefix": "/i3d/"}}`,
		"root prefix":                          `{"etcd": {"endpoints": ["e:2379"], "prefix": "/"}}`,
		"username without password":            `{"etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/", "username": "i3d"}}`,
		"max_workers too large":                `{"max_workers": 1025, "etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/"}}`,
		"bad log level":                        `{"log_level": "loud", "etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/"}}`,
		"plaintext grpc on all interfaces":     `{"grpc_addr": ":7669", "etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/"}}`,
		"pprof on all interfaces":              `{"debug_addr": ":6060", "etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/"}}`,
		"grpc tls without key":                 `{"grpc_tls": {"cert_file": "c"}, "etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/"}}`,
		"etcd cert without key":                `{"etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/", "tls": {"cert_file": "c"}}}`,
		"bad grpc addr":                        `{"grpc_addr": "nope", "etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(PasswordEnv, "")
			os.Unsetenv(PasswordEnv)
			_, err := Parse([]byte(js))
			assert.Error(t, err)
		})
	}
}

func TestAccepts(t *testing.T) {
	for name, js := range map[string]string{
		"plaintext grpc with explicit opt-in": `{"grpc_addr": ":7669", "allow_insecure_grpc": true, "etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/"}}`,
		"grpc tls on all interfaces":          `{"grpc_addr": ":7669", "grpc_tls": {"cert_file": "c", "key_file": "k"}, "etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/"}}`,
		"pprof on loopback":                   `{"debug_addr": "localhost:6060", "etcd": {"endpoints": ["e:2379"], "prefix": "/i3d/"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(js))
			assert.NoError(t, err)
		})
	}
}

func TestIsLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:1": true, "[::1]:1": true, "localhost:1": true,
		":1": false, "0.0.0.0:1": false, "[::]:1": false, "10.0.0.1:1": false, "bad": false,
	} {
		assert.Equal(t, want, IsLoopback(addr), addr)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.json"))
	assert.Error(t, err)
}
