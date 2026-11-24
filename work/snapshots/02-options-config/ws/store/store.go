package store

const MaxBatchSize = 1000

type OfflineOverflowPolicy string

const (
	OfflineOverflowRejectNew   OfflineOverflowPolicy = "reject-new"
	OfflineOverflowEvictOldest OfflineOverflowPolicy = "evict-oldest"
)

type StateStore interface{}
