package db

import "encoding/json"

type Encoder interface {
	Encode(M) ([]byte, error)
}

type Decoder interface {
	Decode([]byte, any) error
}

type JSONEncoder struct{}

func (JSONEncoder) Encode(data M) ([]byte, error) {
	return json.Marshal(data)
}

type JSONDecoder struct{}

func (JSONDecoder) Decode(b []byte, v any) error {
	return json.Unmarshal(b, &v)
}
