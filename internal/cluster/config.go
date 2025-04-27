package cluster

import (
	"encoding/json"
	"os"
)

// Node describe a cada proceso del clúster.
type Node struct {
	ID   string `json:"id"`   // ej.: n1, n2 …
	Host string `json:"host"` // host:port donde escucha gRPC
}

// Config se carga desde cluster.yaml.
type Config struct {
	Nodes []Node `json:"nodes"`
}

// Load lee el archivo YAML/JSON indicado y lo decodifica.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	return &c, json.Unmarshal(b, &c)
}

// Self devuelve la entrada del nodo que coincide con selfID.
func (c *Config) Self(selfID string) *Node {
	for i := range c.Nodes {
		if c.Nodes[i].ID == selfID {
			return &c.Nodes[i]
		}
	}
	return nil
}
