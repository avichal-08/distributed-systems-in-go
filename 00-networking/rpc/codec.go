package rpc

import "encoding/json"

type Request struct {
	ID      uint64          `json:"id"`
	Method  string          `json:"method"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type Response struct {
	ID      uint64          `json:"id"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Error   string          `json:"error,omitempty"`
}

type Codec interface {
	EncodeRequest(Request) ([]byte, error)
	DecodeRequest([]byte) (Request, error)
	EncodeResponse(Response) ([]byte, error)
	DecodeResponse([]byte) (Response, error)
}

type JSONCodec struct{}

func (JSONCodec) EncodeRequest(req Request) ([]byte, error)    { return json.Marshal(req) }
func (JSONCodec) EncodeResponse(resp Response) ([]byte, error) { return json.Marshal(resp) }

func (JSONCodec) DecodeRequest(data []byte) (Request, error) {
	var req Request
	err := json.Unmarshal(data, &req)
	return req, err
}

func (JSONCodec) DecodeResponse(data []byte) (Response, error) {
	var resp Response
	err := json.Unmarshal(data, &resp)
	return resp, err
}
