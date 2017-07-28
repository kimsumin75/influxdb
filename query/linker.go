package query

import (
	"regexp"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
)

type ShardGroup interface {
	MeasurementsByRegex(re *regexp.Regexp) []string
	FieldDimensions(measurements []string) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	MapType(measurement, field string) influxql.DataType
	CreateIterator(measurement string, opt influxql.IteratorOptions) (influxql.Iterator, error)
}

type Linker struct {
	MetaClient interface {
		ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error)
	}

	TSDBStore interface {
		ShardGroup(ids []uint64) ShardGroup
	}

	ShardMapper interface {
		MapShards(m *influxql.Measurement, opt *influxql.SelectOptions) (Database, error)
	}
}
