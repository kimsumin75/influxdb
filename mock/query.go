package mock

import (
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

type Database struct {
	Measurements []string
	Shard        tsdb.ShardGroup
}

func (d *Database) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	itrs := make([]influxql.Iterator, 0, len(d.Measurements))
	for _, name := range d.Measurements {
		itr, err := d.Shard.CreateIterator(name, opt)
		if err != nil {
			influxql.Iterators(itrs).Close()
			return nil, err
		}
		itrs = append(itrs, itr)
	}
	return influxql.NewMergeIterator(itrs, opt), nil
}

func (d *Database) FieldDimensions() (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	return d.Shard.FieldDimensions(d.Measurements)
}

func (d *Database) MapType(field string) influxql.DataType {
	var typ influxql.DataType
	for _, name := range d.Measurements {
		if t := d.Shard.MapType(name, field); typ.LessThan(t) {
			typ = t
		}
	}
	return typ
}

func (d *Database) Close() error {
	return nil
}

type LinkerStub struct {
	ShardsByTimeRangeFn func(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error)
	ShardGroupFn        func(ids []uint64) tsdb.ShardGroup
	MapShardsFn         func(m *influxql.Measurement, opt *influxql.SelectOptions) (query.Database, error)
}

func (l *LinkerStub) ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error) {
	if l.ShardsByTimeRangeFn != nil {
		return l.ShardsByTimeRangeFn(sources, tmin, tmax)
	}
	return nil, nil
}

func (l *LinkerStub) ShardGroup(ids []uint64) tsdb.ShardGroup {
	if l.ShardGroupFn != nil {
		return l.ShardGroupFn(ids)
	}
	return &ShardGroup{}
}

func (l *LinkerStub) MapShards(m *influxql.Measurement, opt *influxql.SelectOptions) (query.Database, error) {
	if l.MapShardsFn != nil {
		return l.MapShardsFn(m, opt)
	}

	shards, err := l.ShardsByTimeRange(influxql.Sources{m}, opt.MinTime, opt.MaxTime)
	if err != nil {
		return nil, err
	}

	ids := make([]uint64, len(shards))
	for i, sh := range shards {
		ids[i] = sh.ID
	}

	var measurements []string
	shard := l.ShardGroup(ids)
	if m.Regex != nil {
		measurements = shard.MeasurementsByRegex(m.Regex.Val)
	} else {
		measurements = []string{m.Name}
	}
	return &Database{
		Measurements: measurements,
		Shard:        shard,
	}, nil
}

type LinkerSetupFn func(stub *LinkerStub)

func NewLinker(fn LinkerSetupFn) *query.Linker {
	stub := LinkerStub{}
	if fn != nil {
		fn(&stub)
	}
	return &query.Linker{
		MetaClient:  &stub,
		TSDBStore:   &stub,
		ShardMapper: &stub,
	}
}
