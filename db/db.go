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
		valueTypeInfo, err := GetValueTypeInfo(v)
		if err != nil {
			return nil, err
		}

		recordBucket.Put([]byte(k), valueTypeInfo.payload)
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
				data[string(k)] = string(v)
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

func GetValueTypeInfo(v any) (ValueTypeInfo, error) {
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
