package main

import (
	"github.com/gorilla/mux"
	"github.com/gorilla/rpc"
	"github.com/gorilla/rpc/json"
	"net/http"
)

import jsonEnc "encoding/json"

type CodeLocation struct {
	Method     string
	Filename   string
	LineNumber int
}

type NestedTrace struct {
	Message  string
	Stack    []CodeLocation
	CausedBy *NestedTrace
}

type LogRequest struct {
	Message    string
	Properties map[string]string
	Trace      NestedTrace
}

type dbFunc func(db *Db)

type LogRequestService struct {
	channel chan dbFunc
}

func CreateLogRequestService() *LogRequestService {
	return &LogRequestService{make(chan dbFunc, 100)}
}

func (s *LogRequestService) ExecuteWithDb(fn func(db *Db)) {
	s.channel <- fn
}

type LogReply struct {
	Message string
}

func (s *LogRequestService) ApplyWithDb(db *Db) {
	for {
		fn := <-s.channel
		fn(db)
	}
}

func (s *LogRequestService) Log(r *http.Request, args *LogRequest, reply *LogReply) error {
	reply.Message = "OK"

	var err error
	var traceAsBytes []byte
	traceAsBytes, err = jsonEnc.Marshal(args.Trace)
	if err != nil {
		return err
	}

	var propertiesAsBytes []byte
	propertiesAsBytes, err = jsonEnc.Marshal(args.Properties)

	propertiesStr := string(propertiesAsBytes)
	traceStr := string(traceAsBytes)

	message := args.Message

	s.ExecuteWithDb(func(db *Db) {
		LogError(db, message, traceStr, propertiesStr)
	})

	return nil
}

func InitRpc(r *mux.Router, filename string) {
	db := OpenDb(filename)

	logReqService := CreateLogRequestService()
	s := rpc.NewServer()
	s.RegisterCodec(json.NewCodec(), "application/json")
	s.RegisterService(logReqService, "Log")

	go logReqService.ApplyWithDb(db)

	r.Handle("/rpc", s)
}
