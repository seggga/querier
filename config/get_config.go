package config

import (
	toml "github.com/pelletier/go-toml"
)

type ConfigType struct {
	Timeout int
	Log     string
	Err     string
}

func GetConfig() (*ConfigType, error) {

	config, err := toml.LoadFile("config/config.toml")
	if err != nil {
		return nil, err
	}

	timeout := config.Get("querier.timeout").(int)
	logFile := config.Get("querier.log").(string)
	errFile := config.Get("querier.err").(string)

	return &ConfigType{Timeout: timeout, Log: logFile, Err: errFile}, nil
}
