package utils

import (
	"context"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server   *ServerConfig   `json:"server,omitempty" yaml:"server"`
	Storage  *StorageConfig  `json:"storage,omitempty" yaml:"storage"`
	Reporter *ReporterConfig `json:"reporter,omitempty" yaml:"reporter"`
}

func (c *Config) Check() bool {
	if c.Server == nil || c.Storage == nil {
		log.Errorf("nil server/storage config")
		return false
	}
	if c.Reporter == nil {
		log.Errorf("nil reporter config")
		return false
	}
	return c.Server.check() && c.Storage.check() && c.Reporter.check()
}

type ServerConfig struct {
	Addr     string `json:"addr,omitempty" yaml:"addr"`
	Name     string `json:"name,omitempty" yaml:"name"`
	LogLevel string `json:"log_level,omitempty" yaml:"log_level"`
	Table    string `json:"table,omitempty" yaml:"table"`
}

func (s *ServerConfig) check() bool {
	if len(s.Addr) == 0 || len(s.Name) == 0 {
		log.Errorf("invalid server parameter")
		return false
	}
	if len(s.LogLevel) == 0 {
		s.LogLevel = "debug"
	}
	if len(s.Table) == 0 {
		log.Errorf("invalid table name")
		return false
	}
	return true
}

type StorageConfig struct {
	Type  StorageType  `json:"type,omitempty" yaml:"type"`
	Redis *RedisConfig `json:"redis,omitempty" yaml:"redis"`
	MySQL *MySQLConfig `json:"mysql,omitempty" yaml:"mysql"`
	Etcd  *EtcdConfig  `json:"etcd,omitempty" yaml:"etcd"`
}

func (s *StorageConfig) check() bool {
	switch s.Type {
	case TypeRedis:
		if s.Redis == nil || !s.Redis.check() {
			log.Errorf("nil or invalid redis config")
			return false
		}
	case TypeMysql:
		if s.MySQL == nil || !s.MySQL.check() {
			log.Errorf("nil or invalid mysql config")
			return false
		}
	default:
		log.Errorf("invalid storage type")
		return false
	}
	if s.Etcd == nil || !s.Etcd.check() {
		log.Errorf("nil or invalid etcd config")
		return false
	}
	return true
}

type RedisConfig struct {
	Network   string `json:"network,omitempty" yaml:"network"`
	Addr      string `json:"addr,omitempty" yaml:"addr"`
	Username  string `json:"username,omitempty" yaml:"username"`
	Password  string `json:"password,omitempty" yaml:"password"`
	Database  int    `json:"database,omitempty" yaml:"database"`
	KeyPrefix string `json:"key_prefix,omitempty" yaml:"key_prefix"`
}

func (r *RedisConfig) check() bool {
	if len(r.Addr) == 0 {
		log.Errorf("invalid redis parameter")
		return false
	}
	if len(r.Network) == 0 {
		r.Network = "tcp"
	}
	if len(r.KeyPrefix) == 0 {
		log.Errorf("invalid redis key prefix")
		return false
	}
	return true
}

type MySQLConfig struct {
	Host         string `json:"host,omitempty" yaml:"host"`
	Port         int    `json:"port,omitempty" yaml:"port"`
	Protocol     string `json:"protocol,omitempty" yaml:"protocol"`
	Database     string `json:"database,omitempty" yaml:"database"`
	Username     string `json:"username,omitempty" yaml:"username"`
	Password     string `json:"password,omitempty" yaml:"password"`
	Charset      string `json:"charset,omitempty" yaml:"charset"`
	PingInterval int    `json:"ping_interval,omitempty" yaml:"ping_interval"`
	MaxIdleConn  int    `json:"max_idle_conn,omitempty" yaml:"max_idle_conn"`
	MaxOpenConn  int    `json:"max_open_conn,omitempty" yaml:"max_open_conn"`
}

func (m *MySQLConfig) check() bool {
	if len(m.Host) == 0 || m.Port == 0 || len(m.Database) == 0 || len(m.Username) == 0 || len(m.Password) == 0 {
		log.Errorf("invalid mysql parameter")
		return false
	}
	if len(m.Protocol) == 0 {
		m.Protocol = "tcp"
	}
	if len(m.Charset) == 0 {
		m.Charset = "utf8"
	}
	if m.PingInterval == 0 {
		m.PingInterval = 60
	}
	if m.MaxIdleConn == 0 {
		m.MaxIdleConn = 100
	}
	if m.MaxOpenConn == 0 {
		m.MaxOpenConn = 200
	}
	return true
}

type EtcdConfig struct {
	Endpoints string `json:"endpoints,omitempty" yaml:"endpoints"`
	Identity  string `json:"identity,omitempty" yaml:"identity"`
}

func (e *EtcdConfig) check() bool {
	if len(e.Endpoints) == 0 {
		log.Errorf("invalid etcd endpoints")
		return false
	}
	if len(e.Identity) == 0 {
		e.Identity = os.Getenv(defaultAllocEnv)
		if len(e.Identity) == 0 {
			log.Errorf("invalid etcd identity")
			return false
		}
	}
	return true
}

type ReporterConfig struct {
	Webhook string `json:"webhook,omitempty" yaml:"webhook"`
	Secret  string `json:"secret,omitempty" yaml:"secret"`
}

func (r *ReporterConfig) check() bool {
	if len(r.Webhook) == 0 || len(r.Secret) == 0 {
		log.Errorf("invalid webhook or secret")
		return false
	}
	return true
}

func NewTestConfig() *Config {
	return &Config{
		Server: &ServerConfig{
			Addr:     ":8080",
			Name:     "sardine",
			LogLevel: "debug",
			Table:    "test",
		},
		Storage: &StorageConfig{
			Type: defaultStoreType,
			Redis: &RedisConfig{
				Network:   "tcp",
				Addr:      "127.0.0.1:6379",
				KeyPrefix: defaultPrefix,
			},
			MySQL: &MySQLConfig{
				Host:         "127.0.0.1",
				Port:         3306,
				Protocol:     "tcp",
				Database:     "leaf",
				Username:     "hans",
				Password:     "123456",
				Charset:      "utf8",
				PingInterval: 60,
				MaxIdleConn:  100,
				MaxOpenConn:  200,
			},
			Etcd: &EtcdConfig{
				Endpoints: "127.0.0.1:2379",
				Identity:  "192.168.0.1",
			},
		},
		Reporter: &ReporterConfig{
			Webhook: "",
			Secret:  "",
		},
	}
}

func (c *Config) GetType() StorageType {
	return c.Storage.Type
}

func (c *Config) GetMySQLSource() string {
	mysql := c.Storage.MySQL
	return fmt.Sprintf("%s:%s@%s(%s:%d)/%s?charset=%s&parseTime=true&loc=Local",
		mysql.Username,
		mysql.Password,
		mysql.Protocol,
		mysql.Host,
		mysql.Port,
		mysql.Database,
		mysql.Charset)
}

func (c *Config) GetTable() string {
	return c.Server.Table
}

func (c *Config) GetEtcdId() string {
	return c.Storage.Etcd.Identity
}

func NewConfig(ctx context.Context, path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		log.Errorf("os open err %+v", err)
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()

	cfg := &Config{}
	err = yaml.NewDecoder(f).Decode(cfg)
	if err != nil {
		log.Errorf("yaml decode err %+v", err)
		return nil, err
	}

	if !cfg.Check() {
		log.Errorf("check config failed")
		return nil, ErrInvalidParameter
	}

	return cfg, nil
}
