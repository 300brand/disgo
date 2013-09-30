package logger

import (
	"github.com/wsxiaoys/terminal"
	"io"
	"log"
)

type ColorWriter struct {
	Color  string
	Writer *terminal.TerminalWriter
}

const DefaultFlags = log.LstdFlags | log.Lshortfile | log.Lmicroseconds

var _ io.Writer = ColorWriter{}

var (
	Trace = log.New(NewColorWriter("b"), "TRACE ", DefaultFlags)
	Debug = log.New(NewColorWriter("c"), "DEBUG ", DefaultFlags)
	Info  = log.New(NewColorWriter("g"), "INFO  ", DefaultFlags)
	Warn  = log.New(NewColorWriter("y"), "WARN  ", DefaultFlags)
	Error = log.New(NewColorWriter("r"), "ERROR ", DefaultFlags)
)

func NewColorWriter(color string) ColorWriter {
	return ColorWriter{
		Color:  color,
		Writer: terminal.Stdout,
	}
}

func (w ColorWriter) Write(p []byte) (n int, err error) {
	return w.Writer.Color(w.Color).Write(p)
}
