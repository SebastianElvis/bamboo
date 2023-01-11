package utils

import (
	"encoding/gob"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"time"

	"github.com/gitferry/bamboo/config"
	"github.com/gitferry/bamboo/crypto"
	"github.com/gitferry/bamboo/identity"
	"github.com/gitferry/bamboo/log"
	"github.com/gitferry/bamboo/message"
)

func FindIntSlice(slice []int, val int) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func RandomPick(n int, f int) []int {
	var randomPick []int
	for i := 0; i < f; i++ {
		var randomID int
		exists := true
		for exists {
			s := rand.NewSource(time.Now().UnixNano())
			r := rand.New(s)
			randomID = r.Intn(n)
			exists = FindIntSlice(randomPick, randomID)
		}
		randomPick = append(randomPick, randomID)
	}
	return randomPick
}

// Max of two int
func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// VMax of a vector
func VMax(v ...int) int {
	max := v[0]
	for _, i := range v {
		if max < i {
			max = i
		}
	}
	return max
}

// Retry function f sleep time between attempts
func Retry(f func() error, attempts int, sleep time.Duration) error {
	var err error
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return nil
		}

		if i >= attempts-1 {
			break
		}

		// exponential delay
		time.Sleep(sleep * time.Duration(i+1))
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

// Schedule repeatedly call function with intervals
func Schedule(f func(), delay time.Duration) chan bool {
	stop := make(chan bool)

	go func() {
		for {
			f()
			select {
			case <-time.After(delay):
			case <-stop:
				return
			}
		}
	}()

	return stop
}

// ConnectToMaster connects to master node and set global Config
func ConnectToMaster(addr string, client bool, id identity.NodeID) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	dec := gob.NewDecoder(conn)
	enc := gob.NewEncoder(conn)
	msg := &message.Register{
		ID:     id,
		Client: client,
		Addr:   "",
	}
	enc.Encode(msg)
	err = dec.Decode(&config.Configuration)
	if err != nil {
		log.Fatal(err)
	}
}

func MapRandomKeyGet(mapI interface{}) interface{} {
	keys := reflect.ValueOf(mapI).MapKeys()

	return keys[rand.Intn(len(keys))].Interface()
}

func IdentifierFixture() crypto.Identifier {
	var id crypto.Identifier
	_, _ = rand.Read(id[:])
	return id
}
