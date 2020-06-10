package jsonrpc2

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"reflect"
	"sync"
)

const jsonrpc2version = "2.0"

var null = []byte("null")

const (
	jsonrpc2codeParseError     = -32700
	jsonrpc2codeInvalidRequest = -32600
	jsonrpc2codeMethodNotFound = -32601
	jsonrpc2codeInvalidParams  = -32602
	jsonrpc2codeInternalError  = -32603
)

type Server struct {
	handlers map[string]interface{}
}

// NewServer returns new JSON RPC 2.0 server.
// It accepts the table of method => handler function.
// A handler function must be of the following format:
// func(ctx, Params) (Result, error) or
// func(ctx) (Result, error) (for the case with no params).
// Params must be a slice, a map or a pointer to a struct.
// Result can be any type serializable to JSON.
func NewServer(handlers map[string]interface{}) *Server {
	return &Server{
		handlers: handlers,
	}
}

func (s *Server) ServeTCP(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go s.serveTCP(conn)
	}
}

type jsonrpc2request struct {
	Jsonrpc string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Id      json.RawMessage `json:"id,omitempty"`
}

type jsonrpc2error struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type jsonrpc2response struct {
	Jsonrpc string          `json:"jsonrpc"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *jsonrpc2error  `json:"error,omitempty"`
	Id      json.RawMessage `json:"id"`
}

func (s *Server) serveTCP(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("Failed to close the connection: %v.", err)
		}
	}()

	p := pushList{alive: true}
	defer func() {
		p.mu.Lock()
		p.alive = false
		p.mu.Unlock()
	}()

	ctx := context.Background()
	ctx = context.WithValue(ctx, pushKey{}, &p)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	decoder := json.NewDecoder(conn)
	decoder.DisallowUnknownFields()
	encoder := json.NewEncoder(conn)

	for {
		// Send all push notifications if they exist.
		p.mu.Lock()
		pushes := p.pushes
		p.pushes = nil
		p.mu.Unlock()
		for _, push := range pushes {
			if err := encoder.Encode(push); err != nil {
				log.Printf("Failed to send push notification: %v.", err)
				return
			}
		}

		var line json.RawMessage
		if err := decoder.Decode(&line); err != nil {
			log.Printf("Failed to read JSON object: %v.", err)
			return
		}
		if line[0] == '[' {
			var batch []jsonrpc2request
			if err := json.Unmarshal(line, &batch); err != nil {
				log.Printf("Failed to parse batch: %v.", err)
				return
			}
			results := make([]*jsonrpc2response, 0, len(batch))
			for _, req := range batch {
				res := s.process(ctx, req)
				if len(req.Id) != 0 && !bytes.Equal(req.Id, null) {
					results = append(results, res)
				}
			}
			if err := encoder.Encode(results); err != nil {
				log.Printf("Failed to write batch results: %v.", err)
				return
			}
		} else if line[0] == '{' {
			var req jsonrpc2request
			if err := json.Unmarshal(line, &req); err != nil {
				log.Printf("Failed to parse request: %v.", err)
				return
			}
			res := s.process(ctx, req)
			if len(req.Id) != 0 && !bytes.Equal(req.Id, null) {
				if err := encoder.Encode(res); err != nil {
					log.Printf("Failed to write result: %v.", err)
					return
				}
			}
		} else {
			log.Printf("Invalid first character in JSON: %s.", line)
			return
		}
	}
}

type pushKey struct{}

type pushList struct {
	mu     sync.Mutex
	pushes []jsonrpc2request
	alive  bool
}

func (s *Server) EnqueueNotification(ctx context.Context, method string, params interface{}) error {
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("failed to marshal params: %w", err)
	}
	req := jsonrpc2request{
		Jsonrpc: jsonrpc2version,
		Method:  method,
		Params:  paramsJSON,
	}

	pi := ctx.Value(pushKey{})
	if pi == nil {
		return fmt.Errorf("ctx does not originate from a call to handler")
	}
	p, ok := pi.(*pushList)
	if !ok {
		panic("wrong type of value for pushKey")
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.alive {
		return fmt.Errorf("can not send notification: session is closed")
	}
	p.pushes = append(p.pushes, req)

	return nil
}

func (s *Server) process(ctx context.Context, req jsonrpc2request) *jsonrpc2response {
	handler, has := s.handlers[req.Method]
	if !has {
		log.Printf("Unknown method: %s.", req.Method)
		return &jsonrpc2response{
			Jsonrpc: jsonrpc2version,
			Id:      req.Id,
			Error: &jsonrpc2error{
				Code:    jsonrpc2codeMethodNotFound,
				Message: fmt.Sprintf("unknown method: %s", req.Method),
			},
		}
	}

	handlerValue := reflect.ValueOf(handler)
	handlerType := handlerValue.Type()
	var resultsValues []reflect.Value
	if handlerType.NumIn() == 1 {
		resultsValues = handlerValue.Call([]reflect.Value{reflect.ValueOf(ctx)})
	} else if handlerType.NumIn() == 2 {
		reqValue := reflect.New(handlerType.In(1))
		if err := json.Unmarshal(req.Params, reqValue.Interface()); err != nil {
			log.Printf("Failed to parse params for method %s: %v.", req.Method, err)
			return &jsonrpc2response{
				Jsonrpc: jsonrpc2version,
				Id:      req.Id,
				Error: &jsonrpc2error{
					Code:    jsonrpc2codeInvalidParams,
					Message: fmt.Sprintf("failed to parse params for method %s: %v", req.Method, err),
				},
			}
		}
		resultsValues = handlerValue.Call([]reflect.Value{reflect.ValueOf(ctx), reqValue.Elem()})
	}

	res := resultsValues[0].Interface()
	err := resultsValues[1].Interface()
	if err != nil {
		log.Printf("Method %s failed: %v.", req.Method, err)
		return &jsonrpc2response{
			Jsonrpc: jsonrpc2version,
			Id:      req.Id,
			Error: &jsonrpc2error{
				Code:    42,
				Message: fmt.Sprintf("method %s failed: %w", req.Method, err),
			},
		}
	}
	return &jsonrpc2response{
		Jsonrpc: jsonrpc2version,
		Id:      req.Id,
		Result:  res,
	}
}
