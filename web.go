package main

import (
	"bytes"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
	"text/template"
)

func applyTemplate(file string, data interface{}) ([]byte, error) {
	var buffer bytes.Buffer

	template, err := template.ParseFiles(file)
	if err != nil {
		return nil, err
	}

	err = template.Execute(&buffer, data)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func renderTemplate(response http.ResponseWriter, file string, data interface{}) {
	buffer, err := applyTemplate(fmt.Sprintf("templates/%s", file), data)
	if err != nil {
		http.Error(response, err.Error(), 500)
	} else {
		response.Write(buffer)
	}
}

// Home: List clusters.  Order by max timestamp
func HomeHandler(response http.ResponseWriter, request *http.Request) {
	db := OpenDb("db.sqlite3")
	clusters, _ := db.GetClusterSummaries()
	renderTemplate(response, "home.html", map[string]interface{}{"clusters": clusters})
	db.Close()
}

// Show cluster: Allow editing of message pattern.   Show all events with that cluster id
// Delete cluster

func InitWeb(r *mux.Router, filename string) {
	r.PathPrefix("/bootstrap/").Handler(http.FileServer(http.Dir(".")))
	r.HandleFunc("/", HomeHandler)
}
