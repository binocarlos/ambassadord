package main

import (
	"log"
	"net/url"
	"strings"

	"github.com/armon/consul-api"
)

type ConsulCacheStore struct {
	client    *consulapi.Client
	waitIndex uint64
}

func NewConsulCacheStore(uri *url.URL) ConfigStore {
	config := consulapi.DefaultConfig()
	if uri.Host != "" {
		config.Address = uri.Host
	}
	client, err := consulapi.NewClient(config)
	assert(err)
	return &ConsulStore{client: client}
}

func (s *ConsulCacheStore) List(name string) []string {
	services, _, err := s.client.Catalog().Service(name, "", &consulapi.QueryOptions{})
	if err != nil {
		log.Println("consul:", err)
		return []string{}
	}
	list := make([]string, 0)
	for _, service := range services {
		list = append(list, service.Address + string(service.ServicePort))
	}
	return list
}

func (s *ConsulCacheStore) Get(name string) string {
	list := s.List(name)
	return strings.Join(list, ",")
}

func (s *ConsulCacheStore) Watch(name string) {
	_, meta, err := s.client.Catalog().Service(name, "", &consulapi.QueryOptions{WaitIndex: s.waitIndex})
	if err != nil {
		log.Println("consul:", err)
	} else {
		s.waitIndex = meta.LastIndex
	}
}
