package disgo

import (
	"bytes"
	"github.com/300brand/logger"
)

type Buffer struct {
	bytes.Buffer
}

func (b Buffer) Close() error {
	logger.Warn.Print("Closing buffer")
	return nil
}
