package disgo

import (
	"encoding/json"
	"fmt"
	"github.com/jbaikge/logger"
	"github.com/mikespook/gearman-go/worker"
	"net/rpc"
	"reflect"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

type NullType struct{}

type Server struct {
	Worker       *worker.Worker
	serviceMutex sync.RWMutex
	serviceMap   map[string]*methodType
	server       *rpc.Server
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
	DefaultServer = NewServer()
	Null          = new(NullType)
	log           = logger.Error
	typeOfError   = reflect.TypeOf((*error)(nil)).Elem() // Ripped from net/rpc
)

// NewServer returns a new Server.
func NewServer() (s *Server) {
	s = &Server{
		Worker:     worker.New(worker.Unlimited),
		server:     rpc.NewServer(),
		serviceMap: make(map[string]*methodType),
	}
	s.Worker.JobHandler = func(j *worker.Job) (err error) {
		logger.Info.Printf("I'm never called :(")
		return
	}
	return
}

// Adds Gearman server addresses to connect to. May be called multiple times.
// A connection is made to each Gearman Job Server, errors may be thrown from
// connectivity issues.
func (s *Server) AddGearman(addrs ...string) (err error) {
	for _, addr := range addrs {
		if err = s.Worker.AddServer(addr); err != nil {
			return
		}
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
	if err = s.server.Register(rcvr); err != nil {
		return
	}
	s.addMethods(rcvr, "")
	return
}

// RegisterName is like Register but uses the provided name for the type
// instead of the receiver's concrete type.
func (s *Server) RegisterName(name string, rcvr interface{}) (err error) {
	if err = s.server.RegisterName(name, rcvr); err != nil {
		return
	}
	return s.addMethods(rcvr, name)
}

// Notifies all Gearman servers of the service methods available and begins
// accepting jobs.
func (s *Server) Serve() {
	for name := range s.serviceMap {
		s.Worker.AddFunc(name, s.handleJob, worker.Immediately)
	}
	s.Worker.Work()
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
	methods := suitableMethods(rcvr, true)
	if len(methods) == 0 {
		return fmt.Errorf("No exported methods found for %s", name)
	}
	for mname, method := range methods {
		fullname := fmt.Sprintf("%s.%s", name, mname)
		if _, ok := s.serviceMap[fullname]; ok {
			return fmt.Errorf("Service method already exists for %s", fullname)
		}
		s.serviceMap[fullname] = method
	}
	return
}

func (s *Server) handleJob(job *worker.Job) (data []byte, err error) {
	logger.Debug.Printf("handling %s", job.Fn)
	method, ok := s.serviceMap[job.Fn]
	if !ok {
		err = fmt.Errorf("Invalid service method: %s", job.Fn)
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
	logger.Info.Printf("disgo: %s took %s", job.Fn, took)

	// error is the only return
	if i := ret[0].Interface(); i != nil {
		err = i.(error)
		logger.Error.Print(err)
		return
	}

	return json.Marshal(reply.Interface())
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
// error using log if reportErr is true.
func suitableMethods(rcvr interface{}, reportErr bool) map[string]*methodType {
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
			if reportErr {
				log.Println("disgo: method", mname, "has wrong number of ins:", mtype.NumIn())
			}
			continue
		}
		// First arg need not be a pointer.
		argType := mtype.In(1)
		if !isExportedOrBuiltinType(argType) {
			if reportErr {
				log.Println("disgo:", mname, "argument type not exported:", argType)
			}
			continue
		}
		// Second arg must be a pointer.
		replyType := mtype.In(2)
		if replyType.Kind() != reflect.Ptr {
			if reportErr {
				log.Println("disgo: method", mname, "reply type not a pointer:", replyType)
			}
			continue
		}
		// Reply type must be exported.
		if !isExportedOrBuiltinType(replyType) {
			if reportErr {
				log.Println("disgo: method", mname, "reply type not exported:", replyType)
			}
			continue
		}
		// Method needs one out.
		if mtype.NumOut() != 1 {
			if reportErr {
				log.Println("disgo: method", mname, "has wrong number of outs:", mtype.NumOut())
			}
			continue
		}
		// The return type of the method must be error.
		if returnType := mtype.Out(0); returnType != typeOfError {
			if reportErr {
				log.Println("disgo: method", mname, "returns", returnType.String(), "not error")
			}
			continue
		}
		methods[mname] = &methodType{rcvr: v, method: method, ArgType: argType, ReplyType: replyType}
	}
	return methods
}
