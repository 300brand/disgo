package disgo

import (
	"encoding/json"
	"fmt"
	"github.com/jbaikge/logger"
	"github.com/mikespook/gearman-go/common"
	"github.com/mikespook/gearman-go/worker"
	"reflect"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

type NullType struct{}

type Server struct {
	ReconnectPause time.Duration
	Worker         *worker.Worker
	addrs          []string
	serviceMutex   sync.RWMutex
	serviceMap     map[string]*methodType
}

// Ripped directly from net/rpc package
type methodType struct {
	sync.Mutex // protects counters
	rcvr       reflect.Value
	method     reflect.Method
	ArgType    reflect.Type
	ReplyType  reflect.Type
	numCalls   uint
}

var (
	// Spits out errors when registering new services
	DebugMode     = false
	DefaultServer = NewServer()
	Null          = new(NullType)
	log           = logger.Error
	typeOfError   = reflect.TypeOf((*error)(nil)).Elem() // Ripped from net/rpc
)

// NewServer returns a new Server.
func NewServer(addrs ...string) (s *Server) {
	s = &Server{
		ReconnectPause: 3 * time.Second,
		addrs:          addrs,
		serviceMap:     make(map[string]*methodType),
	}
	return
}

// Register publishes in the server the set of methods of the
// receiver value that satisfy the following conditions:
//	- exported method
//	- two arguments, both pointers to exported structs
//	- one return value, of type error
// It returns an error if the receiver is not an exported type or has
// no methods or unsuitable methods. It also logs the error using package log.
// The client accesses each method using a string of the form "Type.Method",
// where Type is the receiver's concrete type.
func (s *Server) Register(rcvr interface{}) (err error) {
	return s.addMethods(rcvr, "")
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (s *Server) RegisterName(name string, rcvr interface{}) (err error) {
	return s.addMethods(rcvr, name)
}

// Connects to all gearman addresses, then notifies all Gearman servers of the
// service methods available and begins accepting jobs.
func (s *Server) Serve() (err error) {
	for {
		s.Worker = worker.New(worker.Unlimited)
		s.Worker.ErrHandler = s.errHandler
		connected := 0
		for _, addr := range s.addrs {
			if err = s.Worker.AddServer(addr); err != nil {
				logger.Warn.Printf("disgo.Server: Could add server %s: %s", addr, err)
				continue
			}
			connected++
		}
		// Couldn't find a server to connect to, wait and then try to reconnect
		if connected == 0 {
			logger.Error.Printf("disgo.Server: Couldn't find any servers to connect to. Trying again in %s", s.ReconnectPause)
			<-time.After(s.ReconnectPause)
			continue
		}
		// Connected! Tell the job server we
		for name := range s.serviceMap {
			logger.Trace.Printf("disgo.Server: Adding function %s", name)
			s.Worker.AddFunc(name, s.handleJob, 30)
		}
		logger.Trace.Print("disgo.Server: Starting...")
		s.Worker.Work()
	}
	return
}

// most checks omitted since rpc.Register does them for us
func (s *Server) addMethods(rcvr interface{}, override string) (err error) {
	s.serviceMutex.Lock()
	defer s.serviceMutex.Unlock()

	v := reflect.ValueOf(rcvr)
	name := reflect.Indirect(v).Type().Name()
	if override != "" {
		name = override
	}

	// Extract exported methods
	methods := suitableMethods(rcvr)
	if len(methods) == 0 {
		return fmt.Errorf("disgo.Server: No exported methods found for %s", name)
	}
	for mname, method := range methods {
		fullname := fmt.Sprintf("%s.%s", name, mname)
		if _, ok := s.serviceMap[fullname]; ok {
			return fmt.Errorf("disgo.Server: Service method already exists for %s", fullname)
		}
		s.serviceMap[fullname] = method
	}
	return
}

func (s *Server) errHandler(err error) {
	switch err {
	case common.ErrConnection:
		logger.Error.Printf("disgo.Server: Connection Error. Restarting in %s", s.ReconnectPause)
		<-time.After(s.ReconnectPause)
		s.Worker.Close()
	default:
		logger.Error.Print(err)
	}
}

func (s *Server) handleJob(job *worker.Job) (data []byte, err error) {
	logger.Trace.Printf("disgo.Server: [%s] HNDL %s", job.Fn, job.Handle)
	method, ok := s.serviceMap[job.Fn]
	if !ok {
		err = fmt.Errorf("disgo.Server: Invalid service method: %s", job.Fn)
		logger.Error.Print(err)
		return
	}

	// Decode the argument value.
	var arg reflect.Value
	argIsValue := false // if true, need to indirect before calling.
	if method.ArgType.Kind() == reflect.Ptr {
		arg = reflect.New(method.ArgType.Elem())
	} else {
		arg = reflect.New(method.ArgType)
		argIsValue = true
	}
	// arg guaranteed to be a pointer now.
	if err = json.Unmarshal(job.Data, arg.Interface()); err != nil {
		return
	}
	// Return to a value if it's expected
	if argIsValue {
		arg = arg.Elem()
	}

	// Prepare the reply value
	reply := reflect.New(method.ReplyType.Elem())

	method.Lock()
	method.numCalls++
	method.Unlock()

	// Invoke!
	start := time.Now()
	ret := method.method.Func.Call([]reflect.Value{method.rcvr, arg, reply})
	took := time.Since(start)
	logger.Debug.Printf("disgo.Server: [%s] TOOK %s %s", job.Fn, job.Handle, took)

	response := new(ResponseToClient)

	// error is the only return
	if i := ret[0].Interface(); i != nil {
		response.Error = i
	}
	response.Result = reply.Interface()

	data, err = json.Marshal(response)
	logger.Trace.Printf("disgo.Server: [%s] SEND %s %s", job.Fn, job.Handle, data)
	return
}

// Ripped from net/rpc
// Is this an exported - upper case - name?
func isExported(name string) bool {
	rune, _ := utf8.DecodeRuneInString(name)
	return unicode.IsUpper(rune)
}

// Ripped from net/rpc
// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// PkgPath will be non-empty even for an exported type,
	// so we need to check the type name as well.
	return isExported(t.Name()) || t.PkgPath() == ""
}

// Ripped and modified from net/rpc
// suitableMethods returns suitable Rpc methods of typ, it will report
// error using log if DebugMode is true.
func suitableMethods(rcvr interface{}) map[string]*methodType {
	methods := make(map[string]*methodType)
	v := reflect.ValueOf(rcvr)
	typ := reflect.TypeOf(rcvr)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		mname := method.Name
		// Method must be exported.
		if method.PkgPath != "" {
			continue
		}
		// Method needs three ins: receiver, *args, *reply.
		if mtype.NumIn() != 3 {
			if DebugMode {
				log.Println("disgo.Server: method", mname, "has wrong number of ins:", mtype.NumIn())
			}
			continue
		}
		// First arg need not be a pointer.
		argType := mtype.In(1)
		if !isExportedOrBuiltinType(argType) {
			if DebugMode {
				log.Println("disgo.Server:", mname, "argument type not exported:", argType)
			}
			continue
		}
		// Second arg must be a pointer.
		replyType := mtype.In(2)
		if replyType.Kind() != reflect.Ptr {
			if DebugMode {
				log.Println("disgo.Server: method", mname, "reply type not a pointer:", replyType)
			}
			continue
		}
		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			if DebugMode {
				log.Println("disgo.Server: method", mname, "reply type not exported:", replyType)
			}
			continue
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			if DebugMode {
				log.Println("disgo.Server: method", mname, "has wrong number of outs:", mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			if DebugMode {
				log.Println("disgo.Server: method", mname, "returns", returnType.String(), "not error")
			}
			continue
		}
		methods[mname] = &methodType{rcvr: v, method: method, ArgType: argType, ReplyType: replyType}
	}
	return methods
}
