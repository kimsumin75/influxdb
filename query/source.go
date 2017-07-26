package query

import (
	"io"

	"github.com/influxdata/influxdb/influxql"
)

type ShardMapper interface {
	MapShards(m *influxql.Measurement, opt *influxql.SelectOptions) (Database, error)
}

// compiledSource is a source that links together a variable reference with a source.
type compiledSource interface {
	link(m ShardMapper) (storage, error)
}

type Database interface {
	CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error)
	FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(field string) influxql.DataType
	io.Closer
}

// storage is an abstraction over the storage layer.
type storage interface {
	FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(field string) influxql.DataType
	io.Closer
	resolve(ref *influxql.VarRef, out *WriteEdge)
}

type measurement struct {
	stmt   *compiledStatement
	source *influxql.Measurement
}

func (m *measurement) link(shardMapper ShardMapper) (storage, error) {
	opt := influxql.SelectOptions{
		MinTime: m.stmt.TimeRange.Min,
		MaxTime: m.stmt.TimeRange.Max,
	}
	shard, err := shardMapper.MapShards(m.source, &opt)
	if err != nil {
		return nil, err
	}
	return &storageEngine{
		stmt:  m.stmt,
		shard: shard,
	}, nil
}

type storageEngine struct {
	stmt  *compiledStatement
	shard Database
}

func (s *storageEngine) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	return s.shard.FieldDimensions()
}

func (s *storageEngine) MapType(field string) influxql.DataType {
	return s.shard.MapType(field)
}

func (s *storageEngine) resolve(ref *influxql.VarRef, out *WriteEdge) {
	ic := &IteratorCreator{
		Expr:            ref,
		AuxiliaryFields: s.stmt.AuxiliaryFields,
		Database:        s.shard,
		Dimensions:      s.stmt.Dimensions,
		Tags:            s.stmt.Tags,
		TimeRange:       s.stmt.TimeRange,
		Output:          out,
	}
	out.Node = ic
}

func (s *storageEngine) Close() error {
	return s.shard.Close()
}

type storageEngines []storage

func (a storageEngines) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})

	for _, s := range a {
		f, d, err := s.FieldDimensions()
		if err != nil {
			return nil, nil, err
		}

		for k, typ := range f {
			if _, ok := fields[k]; typ != influxql.Unknown && (!ok || typ < fields[k]) {
				fields[k] = typ
			}
		}
		for k := range d {
			dimensions[k] = struct{}{}
		}
	}
	return fields, dimensions, nil
}

func (a storageEngines) MapType(field string) influxql.DataType {
	var typ influxql.DataType
	for _, s := range a {
		if t := s.MapType(field); typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (a storageEngines) resolve(ref *influxql.VarRef, out *WriteEdge) {
	merge := &Merge{Output: out}
	out.Node = merge
	for _, s := range a {
		out := merge.AddInput(nil)
		s.resolve(ref, out)
	}
}

func (a storageEngines) Close() error {
	for _, s := range a {
		s.Close()
	}
	return nil
}
