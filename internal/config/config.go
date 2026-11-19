// Package config loads and validates the etcdflake server configuration.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
)

// PasswordEnv is the environment variable holding the etcd password. It takes
// precedence over Etcd.PasswordFile.
const PasswordEnv = "ETCDFLAKE_ETCD_PASSWORD" //nolint:gosec // variable name, not a credential

// Config is the server configuration, loaded from a JSON file.
type Config struct {
	// GRPCAddr is the gRPC listen address. Default "127.0.0.1:7669".
	GRPCAddr string `json:"grpc_addr"`

	// GRPCTLS enables TLS on the gRPC listener. Set ClientCAFile for mutual TLS.
	GRPCTLS TLS `json:"grpc_tls"`

	// AllowInsecureGRPC allows plaintext gRPC on a non-loopback address. Only set
	// it when the network is private, e.g. a cluster-internal Service.
	AllowInsecureGRPC bool `json:"allow_insecure_grpc"`

	// HTTPAddr serves /metrics, /livez and /readyz. Default "127.0.0.1:8001".
	HTTPAddr string `json:"http_addr"`

	// DebugAddr serves /debug/pprof when set. Must be a loopback address.
	DebugAddr string `json:"debug_addr"`

	// LogLevel is one of debug, info, warn, error. Default "info".
	LogLevel string `json:"log_level"`

	// MaxWorkers is the number of worker IDs the cluster shares, [1, 1024]. Default 1024.
	// Every node in a cluster must use the same value.
	MaxWorkers int `json:"max_workers"`

	Etcd Etcd `json:"etcd"`
}

// Etcd configures the etcd connection.
type Etcd struct {
	Endpoints []string `json:"endpoints"`

	// Prefix namespaces all keys, e.g. "/etcdflake/". One prefix is one cluster.
	Prefix string `json:"prefix"`

	Username string `json:"username"`

	// PasswordFile is a file containing the password. Ignored when $ETCDFLAKE_ETCD_PASSWORD is set.
	PasswordFile string `json:"password_file"`

	// TLS for the etcd connection. Set CertFile and KeyFile for client certificate auth.
	TLS TLS `json:"tls"`

	// Password is resolved from PasswordEnv or PasswordFile. Never read from JSON.
	Password string `json:"-"`
}

// TLS holds certificate file paths.
type TLS struct {
	CertFile     string `json:"cert_file"`
	KeyFile      string `json:"key_file"`
	CAFile       string `json:"ca_file"`        // etcd: CA to verify the server
	ClientCAFile string `json:"client_ca_file"` // gRPC: CA to verify clients (mTLS)
}

// Enabled reports whether any TLS setting is present.
func (t TLS) Enabled() bool {
	return t.CertFile != "" || t.KeyFile != "" || t.CAFile != "" || t.ClientCAFile != ""
}

// Load reads, defaults and validates the config file at path.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path) //nolint:gosec // the path comes from the operator's command line
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(b)
}

// Parse parses, defaults and validates a JSON config.
func Parse(b []byte) (*Config, error) {
	var c Config
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.setDefaults()
	if err := c.resolvePassword(); err != nil {
		return nil, err
	}
	if err := c.validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	return &c, nil
}

func (c *Config) setDefaults() {
	if c.GRPCAddr == "" {
		c.GRPCAddr = "127.0.0.1:7669"
	}
	if c.HTTPAddr == "" {
		c.HTTPAddr = "127.0.0.1:8001"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.MaxWorkers == 0 {
		c.MaxWorkers = 1024
	}
	if c.Etcd.Prefix != "" && !strings.HasSuffix(c.Etcd.Prefix, "/") {
		c.Etcd.Prefix += "/"
	}
}

func (c *Config) resolvePassword() error {
	if v, ok := os.LookupEnv(PasswordEnv); ok {
		c.Etcd.Password = v
		return nil
	}
	if c.Etcd.PasswordFile == "" {
		return nil
	}
	b, err := os.ReadFile(c.Etcd.PasswordFile)
	if err != nil {
		return fmt.Errorf("read etcd password file: %w", err)
	}
	c.Etcd.Password = strings.TrimRight(string(b), "\r\n")
	return nil
}

func (c *Config) validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if len(c.Etcd.Endpoints) == 0 {
		add("etcd.endpoints is required")
	}
	if c.Etcd.Prefix == "" || c.Etcd.Prefix == "/" {
		add("etcd.prefix is required and must not be the root")
	}
	if c.Etcd.Username != "" && c.Etcd.Password == "" {
		add("etcd.username is set but no password: set $%s or etcd.password_file", PasswordEnv)
	}
	if (c.Etcd.TLS.CertFile == "") != (c.Etcd.TLS.KeyFile == "") {
		add("etcd.tls.cert_file and etcd.tls.key_file must be set together")
	}
	if c.Etcd.TLS.ClientCAFile != "" {
		add("etcd.tls.client_ca_file is not used; did you mean ca_file?")
	}
	if c.MaxWorkers < 1 || c.MaxWorkers > 1024 {
		add("max_workers %d out of range [1, 1024]", c.MaxWorkers)
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		add("log_level %q must be one of debug, info, warn, error", c.LogLevel)
	}

	t := c.GRPCTLS
	if t.Enabled() && (t.CertFile == "" || t.KeyFile == "") {
		add("grpc_tls.cert_file and grpc_tls.key_file are required when grpc_tls is set")
	}
	if t.CAFile != "" {
		add("grpc_tls.ca_file is not used; did you mean client_ca_file?")
	}
	for name, addr := range map[string]string{"grpc_addr": c.GRPCAddr, "http_addr": c.HTTPAddr} {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			add("%s %q: %v", name, addr, err)
		}
	}
	if !t.Enabled() && !c.AllowInsecureGRPC && !IsLoopback(c.GRPCAddr) {
		add("grpc_addr %q is not loopback: configure grpc_tls, or set allow_insecure_grpc on a private network", c.GRPCAddr)
	}
	if c.DebugAddr != "" && !IsLoopback(c.DebugAddr) {
		add("debug_addr %q must be a loopback address because pprof is unauthenticated", c.DebugAddr)
	}
	return errors.Join(errs...)
}

// IsLoopback reports whether a host:port address binds only to loopback.
// An empty host means all interfaces and is not loopback.
func IsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
