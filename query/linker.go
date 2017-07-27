package query

import (
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
)

type Linker struct {
	MetaClient interface {
		ShardsByTimeRange(sources influxql.Sources, tmin, tmax time.Time) (a []meta.ShardInfo, err error)
	}

	TSDBStore interface {
		ShardGroup(ids []uint64) tsdb.ShardGroup
	}

	ShardMapper interface {
		MapShards(m *influxql.Measurement, opt *influxql.SelectOptions) (Database, error)
	}
}
