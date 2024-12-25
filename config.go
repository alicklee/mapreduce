package mapreduce

import (
	"log"
	"os"

	"gopkg.in/yaml.v2"
)

var Config map[string]string

func init() {
	var config map[string]map[string]string
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}
	Config = config["paths"]
}
