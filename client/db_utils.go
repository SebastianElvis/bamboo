package main

import "github.com/gitferry/bamboo/db"

// Database implements bamboo.DB interface for benchmarking
type Database struct {
	Client
}

func (d *Database) Init() error {
	return nil
}

func (d *Database) Stop() error {
	return nil
}

func (d *Database) Write(k int, v []byte) error {
	key := db.Key(k)
	err := d.Put(key, v)
	return err
}
