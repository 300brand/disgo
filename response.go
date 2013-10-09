package disgo

import (
	"encoding/json"
)

type ResponseToClient struct {
	Error  interface{} `json:"error"`
	Id     uint64      `json:"id"`
	Result interface{} `json:"result"`
}

type ResponseFromServer struct {
	Error  interface{}      `json:"error"`
	Id     uint64           `json:"id"`
	Result *json.RawMessage `json:"result"`
}
