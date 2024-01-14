package db

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"

	"github.com/google/uuid"
	"go.etcd.io/bbolt"
)

const (
	dbName = "default.db"
)

type ValueType int

func (v ValueType) String() string {
	switch v {
	case ValueTypeString:
		return "string"
	case ValueTypeInt:
		return "int"
	case ValueTypeBool:
		return "bool"
	case ValueTypeFloat:
		return "float"
	case ValueTypeUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

const (
	ValueTypeUnknown ValueType = iota
	ValueTypeString
	ValueTypeInt
	ValueTypeBool
	ValueTypeFloat
)

type M map[string]any

type Filter struct {
	EQ    map[string]any
	Limit int
	Sort  string
}

type Db struct {
	db *bbolt.DB
}

type Collection struct {
	*bbolt.Bucket
}

func New() (*Db, error) {
	db, err := bbolt.Open(dbName, 0666, nil)
	if err != nil {
		return nil, err
	}

	return &Db{
		db: db,
	}, nil
}

func (d *Db) CreateCollection(name string) (*Collection, error) {
	tx, err := d.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists([]byte(name))
	if err != nil {
		return nil, err
	}

	return &Collection{
		Bucket: bucket,
	}, nil
}

func (d *Db) Insert(collName string, data M) (*uuid.UUID, error) {
	id := uuid.New()

	tx, err := d.db.Begin(true)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	bucket, err := tx.CreateBucketIfNotExists([]byte(collName))
	if err != nil {
		return nil, err
	}

	recordBucket, err := bucket.CreateBucketIfNotExists([]byte(id.String()))
	if err != nil {
		return nil, err
	}

	for k, v := range data {
		valueTypeInfo, err := getValueTypeInfo(v)
		if err != nil {
			return nil, err
		}

		if err := recordBucket.Put([]byte(k+"_"+valueTypeInfo.valueType.String()), valueTypeInfo.payload); err != nil {
			return nil, err
		}
	}

	return &id, tx.Commit()

}

func (d *Db) Select(coll string, filter Filter) ([]M, error) {
	tx, err := d.db.Begin(false)
	if err != nil {
		return nil, err
	}
	bucket := tx.Bucket([]byte(coll))
	if bucket == nil {
		return nil, fmt.Errorf("collection %s not found", coll)
	}

	results := []M{}
	bucket.ForEach(func(k, v []byte) error {
		if v == nil {
			entryBucket := bucket.Bucket(k)
			if entryBucket == nil {
				return fmt.Errorf("bucket %s not found", k)
			}

			data := M{}
			entryBucket.ForEach(func(k, v []byte) error {
				key_type := string(k)
				key := key_type[:len(key_type)-3]
				valueType := key_type[len(key_type)-2:]
				val, err := getValueFromBytes(valueType, v)
				if err != nil {
					return err
				}
				data[key] = val
				return nil
			})
			include := true

			if filter.EQ != nil {
				include = false
				for fk, fv := range filter.EQ {
					if value, ok := data[fk]; ok {
						if value == fv {
							include = true
						}
					}
				}
			}
			if include {
				results = append(results, data)
			}
		}
		return nil
	})
	return results, tx.Commit()
}

func (d *Db) Close() error {
	return d.db.Close()
}

type ValueTypeInfo struct {
	valueType ValueType
	payload   []byte
}

func getValueTypeInfo(v any) (ValueTypeInfo, error) {
	switch it := v.(type) {
	case string:
		return ValueTypeInfo{
			valueType: ValueTypeString,
			payload:   []byte(it),
		}, nil
	case int:
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, uint32(it))
		return ValueTypeInfo{
			valueType: ValueTypeInt,
			payload:   b,
		}, nil
	case float64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, math.Float64bits(it))
		return ValueTypeInfo{
			valueType: ValueTypeFloat,
			payload:   b,
		}, nil
	case bool:
		b := make([]byte, 1)
		if it {
			b[0] = 1
		} else {
			b[0] = 0
		}
		return ValueTypeInfo{
			valueType: ValueTypeBool,
			payload:   b,
		}, nil
	default:
		return ValueTypeInfo{
			valueType: ValueTypeUnknown,
		}, fmt.Errorf("unsupported type %s", reflect.TypeOf(v))
	}
}

func getValueFromBytes(valueType string, payload []byte) (any, error) {
	switch valueType {
	case "0":
		return string(payload), nil
	case "1":
		return int(binary.LittleEndian.Uint32(payload)), nil
	case "2":
		return math.Float64frombits(binary.LittleEndian.Uint64(payload)), nil
	case "3":
		return payload[0] == 1, nil
	default:
		return nil, fmt.Errorf("unsupported type %s", valueType)
	}
}

// db.Update(func(tx *bbolt.Tx) error {
// 	uuid := uuid.New()

// 	for k, v := range user {
// 		if err := bucket.Put([]byte(k), []byte(v)); err != nil {
// 			return err
// 		}
// 	}

// 	if err := bucket.Put([]byte("id"), []byte(uuid.String())); err != nil {
// 		return err
// 	}

// 	return nil
// })
