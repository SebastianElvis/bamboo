package main

import (
	"flag"
	"net/http"
	"strconv"
	"sync"

	"github.com/gitferry/bamboo/config"
	"github.com/gitferry/bamboo/crypto"
	"github.com/gitferry/bamboo/identity"
	"github.com/gitferry/bamboo/log"
	"github.com/gitferry/bamboo/replica"
)

var id = flag.String("id", "", "NodeID of the node")
var simulation = flag.Bool("sim", false, "simulation mode")

func Init() {
	flag.Parse()
	log.Setup()
	config.Configuration.Load()
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1000
}

func initReplica(id identity.NodeID, isByz bool) {
	log.Infof("node %v starting...", id)
	if isByz {
		log.Infof("node %v is Byzantine", id)
	}

	r := replica.NewReplica(id, config.GetConfig().Algorithm, isByz)
	r.Start()
}

func main() {
	Init()

	// the private and public keys are generated here
	errCrypto := crypto.SetKeys()
	if errCrypto != nil {
		log.Fatal("Could not generate keys:", errCrypto)
	}
	if *simulation {
		var wg sync.WaitGroup
		wg.Add(1)
		config.Simulation()
		for id := range config.GetConfig().Addrs {
			isByz := false
			if id.Node() <= config.GetConfig().ByzNo {
				isByz = true
			}
			go initReplica(id, isByz)
		}
		wg.Wait()
	} else {
		setupDebug()
		isByz := false
		i, _ := strconv.Atoi(*id)
		if i <= config.GetConfig().ByzNo {
			isByz = true
		}
		initReplica(identity.NodeID(*id), isByz)
	}
}
