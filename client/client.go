package main

import (
	"flag"
	"net/http"

	"github.com/gitferry/bamboo/benchmark"
	"github.com/gitferry/bamboo/config"
	"github.com/gitferry/bamboo/log"
)

func Init() {
	flag.Parse()
	log.Setup()
	config.Configuration.Load()
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1000
}

func main() {
	Init()
	d := new(Database)
	d.Client = NewHTTPClient()
	b := benchmark.NewBenchmark(d)
	b.Run()
}
