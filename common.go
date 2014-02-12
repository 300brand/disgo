package disgo

type NullType struct{}

var (
	Null = new(NullType)

	RPCGOB  = []byte(`GOB `)
	RPCJSON = []byte(`JSON`)
	RPCHTTP = []byte(`HTTP`)
)
