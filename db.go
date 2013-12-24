package main

import (
	"code.google.com/p/go-sqlite/go1/sqlite3"
	"fmt"
	"io"
	"os"
	"time"
)

type Log struct {
	Id           int64
	Message      string
	Trace        string
	Properties   map[string]string
	LogTimestamp time.Time
	ClusterId    int64
}

type Cluster struct {
	Id             int64
	Summary        string
	MessagePattern string
	Trace          string
}

var createSql = [...]string{
	"CREATE TABLE ERROR_LOG (LOG_ID INTEGER PRIMARY KEY AUTOINCREMENT, MESSAGE TEXT, TRACE TEXT, PROPERTIES TEXT, LOG_TIMESTAMP INTEGER, CLUSTER_ID INTEGER)",
	"CREATE INDEX ERROR_LOG_CLUSTER_ID ON ERROR_LOG ( CLUSTER_ID )",
	"CREATE TABLE ERROR_CLUSTER (CLUSTER_ID INTEGER PRIMARY KEY AUTOINCREMENT, SUMMARY TEXT, MESSAGE_PATTERN TEXT, TRACE TEXT)"}

type Db struct {
	c *sqlite3.Conn
}

func (db *Db) CreateSchema() (string, error) {
	for _, stmt := range createSql {
		err := db.c.Exec(stmt)
		if err != nil {
			return stmt, err
		}
	}
	return "", nil
}

func OpenDb(filename string) *Db {
	_, exists_err := os.Stat(filename)
	newDb := os.IsNotExist(exists_err)

	c, err := sqlite3.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("Could not open %s: %s", filename, err.Error()))
	}

	db := &Db{c}
	if newDb {
		stmt, err2 := db.CreateSchema()
		if err2 != nil {
			c.Close()
			os.Remove(filename)
			panic(fmt.Sprintf("Could not execute: %s. Got error: %s", stmt, err2.Error()))
		}
	}

	return &Db{c}
}

func (db *Db) Close() error {
	return db.c.Close()
}

func (db *Db) InsertLog(message string, trace string, properties string, timestamp time.Time, clusterId int64) error {
	return db.c.Exec("INSERT INTO ERROR_LOG (MESSAGE, TRACE, PROPERTIES, LOG_TIMESTAMP, CLUSTER_ID) VALUES (?, ?, ?, ?, ?)",
		message, trace, properties, timestamp, clusterId)
}

func (db *Db) GetLogs(clusterId int64) ([]*Log, error) {
	errors := make([]*Log, 0, 100)
	for s, err := db.c.Query("SELECT LOG_ID, MESSAGE, TRACE, PROPERTIES, TRACE, PROPERTIES, LOG_TIMESTAMP, CLUSTER_ID FROM ERROR_LOG WHERE CLUSTER_ID = ?", clusterId); err == nil; err = s.Next() {
		e := new(Log)
		s.Scan(&e.Id, &e.Message, &e.Trace, &e.Properties, &e.LogTimestamp, &e.ClusterId)
		errors = append(errors, e)
	}

	// FIXME, not handling err
	return errors, nil
}

func (db *Db) GetCluster(clusterId int) (*Cluster, error) {
	cluster := new(Cluster)
	s, err := db.c.Query("SELECT CLUSTER_ID, SUMMARY, MESSAGE_PATTERN, TRACE FROM ERROR_CLUSTER WHERE CLUSTER_ID = ?", clusterId)
	s.Scan(&cluster.Id, &cluster.Summary, &cluster.MessagePattern, &cluster.Trace)

	// FIXME err
	return cluster, err
}

type ClusterSummary struct {
	ClusterId int64
	Summary   string
	Count     int
	First     time.Time
	Last      time.Time
}

func (db *Db) GetClusterSummaries() ([]*ClusterSummary, error) {
	summaries := make([]*ClusterSummary, 0, 100)
	s, err := db.c.Query("SELECT c.CLUSTER_ID, c.SUMMARY, agg.ROW_COUNT, agg.FIRST, agg.LAST FROM ERROR_CLUSTER c join (SELECT CLUSTER_ID, COUNT(1) ROW_COUNT, MIN(LOG_TIMESTAMP) FIRST, MAX(LOG_TIMESTAMP) LAST FROM ERROR_LOG GROUP BY CLUSTER_ID) agg on agg.CLUSTER_ID = c.CLUSTER_ID")
	for ; err == nil; err = s.Next() {
		summary := new(ClusterSummary)
		s.Scan(&summary.ClusterId, &summary.Summary, &summary.Count, &summary.First, &summary.Last)
		summaries = append(summaries, summary)
	}

	if err != io.EOF {
		return nil, err
	}

	return summaries, nil
}

func (db *Db) GetAllClusters() ([]*Cluster, error) {
	clusters := make([]*Cluster, 0, 100)

	fmt.Printf("fetch\n")
	s, err := db.c.Query("SELECT CLUSTER_ID, SUMMARY, MESSAGE_PATTERN, TRACE FROM ERROR_CLUSTER")
	for ; err == nil; err = s.Next() {
		fmt.Printf("newCluster\n")
		cluster := new(Cluster)
		s.Scan(&cluster.Id, &cluster.Summary, &cluster.MessagePattern, &cluster.Trace)
		clusters = append(clusters, cluster)
	}

	fmt.Printf("Returning %s", err.Error())

	if err != io.EOF {
		return nil, err
	}

	// FIXME: err
	return clusters, nil
}

func (db *Db) InsertCluster(summary string, message string, trace string) (int64, error) {
	err := db.c.Exec("INSERT INTO ERROR_CLUSTER (SUMMARY, MESSAGE_PATTERN, TRACE) VALUES (?, ?, ?)", summary, message, trace)
	if err != nil {
		return 0, err
	}
	return db.c.LastInsertId(), nil
}

func (db *Db) DeleteCluster(clusterId int64) error {
	err := db.c.Exec("DELETE FROM ERROR_LOG WHERE CLUSTER_ID = ?", clusterId)
	if err != nil {
		return err
	}
	err = db.c.Exec("DELETE FROM ERROR_CLUSTER WHERE CLUSTER_ID = ?", clusterId)
	return err
}
