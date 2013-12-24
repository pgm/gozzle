package main

import (
	"fmt"
	"time"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func (c *Cluster) Matches(message string, trace string) bool {
	result := (c.MessagePattern == message && c.Trace == trace)
	fmt.Printf("Matches(%s, %s) == (%s, %s) -> %s\n", message, trace, c.MessagePattern, c.Trace, result)
	return result
}

func FindFirstMatchingCluster(clusters []*Cluster, message string, trace string) *Cluster {
	for _, c := range clusters {
		if c.Matches(message, trace) {
			return c
		}
	}

	return nil
}

// finds, or creates if necessary cluster id
func DeriveClusterId(db *Db, clusters []*Cluster, message string, trace string) (int64, error) {
	cluster := FindFirstMatchingCluster(clusters, message, trace)
	if cluster == nil {
		// create a new cluster
		return db.InsertCluster(message, message, trace)
	}
	return cluster.Id, nil
}

func LogError(db *Db, message string, trace string, properties string) error {
	var clusters []*Cluster
	var err error

	clusters, err = db.GetAllClusters()
	if err != nil {
		return err
	}

	var clusterId int64
	clusterId, err = DeriveClusterId(db, clusters, message, trace)
	if err != nil {
		return err
	}

	err = db.InsertLog(message, trace, properties, time.Now(), clusterId)
	return err
}

func main() {
	filename := "db.sqlite3"

	r := mux.NewRouter()
	InitRpc(r, filename)
	InitWeb(r, filename)

	err := http.ListenAndServe(":10301", r)
	if err != nil {
		log.Fatal("Could not listen: ", err)
	}
}
