package mock

import (
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
)

type ShardMapper struct {
	MapShardsFn func(m *influxql.Measurement, opt *influxql.SelectOptions) (query.Database, error)
}

func (s *ShardMapper) MapShards(m *influxql.Measurement, opt *influxql.SelectOptions) (query.Database, error) {
	return s.MapShardsFn(m, opt)
}
