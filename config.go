package main

import (
	"github.com/BurntSushi/toml"
	"github.com/joho/godotenv"
	"log"
)

type YdbConfig struct {
	Endpoint string
}

type Configuration struct {
	Ydb YdbConfig `toml:"database"`
}

func GetConfig() Configuration {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Could not read dotenv file", err)
	}
	var config Configuration
	if _, err := toml.DecodeFile("config.toml", &config); err != nil {
		log.Fatal("Could not read config", err)
	}
	return config
}
