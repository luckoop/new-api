package model

import (
	"math"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting/operation_setting"
)

// InferenceLoad is one channel's /metrics snapshot. Samples live only in this
// process: a restart loses them, and the mount warm-up or the next request
// rebuilds them. NaN is never stored because the admin API serializes to JSON.
type InferenceLoad struct {
	ChannelID    int     `json:"channel_id"`
	Waiting      float64 `json:"waiting"`
	Running      float64 `json:"running"`
	KVCacheUsage float64 `json:"kv_cache_usage"`
	CacheHitRate float64 `json:"cache_hit_rate"`
	SampledAt    int64   `json:"sampled_at"` // Unix seconds; the frontend's relative-time helpers expect seconds
	OK           bool    `json:"ok"`
}

type prefixCacheCounter struct {
	queries float64
	hits    float64
	rate    float64
}

var (
	inferenceLoadsMu     sync.RWMutex
	inferenceLoads       = make(map[int]InferenceLoad)
	inferenceRefreshCh   = make(chan int, 4096)
	inferenceLastRefresh sync.Map // channel id -> int64 unix milliseconds
	prefixCacheMu        sync.Mutex
	prefixCacheBaselines = make(map[int]prefixCacheCounter)
)

// InferenceRefreshQueue is consumed by the controller's sampler. It is
// intentionally buffered and non-blocking on the producer side: losing a
// refresh hint only delays a sample, it never delays a user request.
func InferenceRefreshQueue() <-chan int {
	return inferenceRefreshCh
}

// SetInferenceLoad stores a snapshot. A failed sample may be stored once so
// the admin UI can show a stale channel, but callers usually keep the previous
// value instead (see sampleOneInferenceChannel).
func SetInferenceLoad(load InferenceLoad) {
	inferenceLoadsMu.Lock()
	defer inferenceLoadsMu.Unlock()
	inferenceLoads[load.ChannelID] = load
}

// GetInferenceLoad returns the current snapshot for one channel.
func GetInferenceLoad(channelID int) (InferenceLoad, bool) {
	inferenceLoadsMu.RLock()
	defer inferenceLoadsMu.RUnlock()
	load, ok := inferenceLoads[channelID]
	return load, ok
}

// GetAllInferenceLoads returns a copy of every snapshot for the admin API.
func GetAllInferenceLoads() []InferenceLoad {
	inferenceLoadsMu.RLock()
	defer inferenceLoadsMu.RUnlock()
	loads := make([]InferenceLoad, 0, len(inferenceLoads))
	for _, load := range inferenceLoads {
		loads = append(loads, load)
	}
	return loads
}

// GetEnabledInferenceChannels lists vLLM/SGLang channels that can serve
// traffic. The database path is used first because the memory channel cache is
// optional (MEMORY_CACHE_ENABLED defaults to false); the cache is only a
// fallback for tests and startup before DB initialization.
func GetEnabledInferenceChannels() []int {
	if DB != nil {
		var ids []int
		if err := DB.Model(&Channel{}).
			Where("status = ? AND type IN ?", common.ChannelStatusEnabled,
				[]int{constant.ChannelTypeVLLM, constant.ChannelTypeSGLang}).
			Pluck("id", &ids).Error; err == nil {
			return ids
		}
	}
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()
	ids := make([]int, 0, len(channelsIDM))
	for id, channel := range channelsIDM {
		if channel == nil || channel.Status != common.ChannelStatusEnabled {
			continue
		}
		if channel.Type != constant.ChannelTypeVLLM && channel.Type != constant.ChannelTypeSGLang {
			continue
		}
		ids = append(ids, id)
	}
	return ids
}

// TryRefreshInferenceChannels enqueues a sample hint for each channel whose
// per-channel cooldown has elapsed. It never blocks: the routing hot path calls
// it on every request, so a full queue must drop hints rather than stall.
//
// Boundary: the CAS on inferenceLastRefresh guarantees at most one in-flight
// hint per channel per cooldown window across all three gateway replicas in a
// process. A dropped hint releases its slot so the next request can retry.
func TryRefreshInferenceChannels(channelIDs []int) {
	setting := operation_setting.GetInferenceRoutingSetting()
	if !setting.Enabled {
		return
	}
	now := time.Now().UnixMilli()
	cooldown := int64(setting.QueueRefreshCooldownSeconds) * 1000
	for _, id := range channelIDs {
		if id <= 0 {
			continue
		}
		previous, loaded := inferenceLastRefresh.Load(id)
		if loaded {
			last, ok := previous.(int64)
			if ok && now-last < cooldown {
				continue
			}
			if !inferenceLastRefresh.CompareAndSwap(id, previous, now) {
				continue
			}
		} else if _, loaded := inferenceLastRefresh.LoadOrStore(id, now); loaded {
			continue
		}
		select {
		case inferenceRefreshCh <- id:
		default:
			inferenceLastRefresh.Store(id, int64(0))
		}
	}
}

// RefreshAllInferenceChannels warms every inference channel at mount time,
// bypassing the cooldown so the first requests have a usable snapshot.
func RefreshAllInferenceChannels() {
	for _, id := range GetEnabledInferenceChannels() {
		select {
		case inferenceRefreshCh <- id:
		default:
		}
	}
}

// RefreshStaleInferenceLoads re-samples channels with no snapshot or a sample
// older than the validity window. The admin list polls this endpoint, which is
// what keeps the UI fresh without a resident ticker.
func RefreshStaleInferenceLoads() {
	setting := operation_setting.GetInferenceRoutingSetting()
	if !setting.Enabled {
		return
	}
	now := time.Now().Unix()
	validity := int64(setting.SampleIntervalSeconds)
	for _, id := range GetEnabledInferenceChannels() {
		load, ok := GetInferenceLoad(id)
		if ok && load.OK && now-load.SampledAt < validity {
			continue
		}
		TryRefreshInferenceChannels([]int{id})
	}
}

// UpdatePrefixCacheStats converts vLLM's monotonic prefix-cache counters into
// a hit rate. The first sample uses the lifetime rate; later samples use the
// counter delta. A counter reset (upstream restart) or no progress returns
// ok=false so the caller keeps the last known rate instead of flashing 0%.
func UpdatePrefixCacheStats(channelID int, queries float64, hits float64) (float64, bool) {
	if channelID <= 0 || math.IsNaN(queries) || math.IsNaN(hits) || queries <= 0 || hits < 0 {
		return 0, false
	}
	prefixCacheMu.Lock()
	defer prefixCacheMu.Unlock()
	previous, exists := prefixCacheBaselines[channelID]
	if !exists {
		rate := min(1, hits/queries)
		prefixCacheBaselines[channelID] = prefixCacheCounter{queries: queries, hits: hits, rate: rate}
		return rate, true
	}
	if queries < previous.queries || hits < previous.hits {
		prefixCacheBaselines[channelID] = prefixCacheCounter{queries: queries, hits: hits, rate: previous.rate}
		return previous.rate, false
	}
	deltaQueries := queries - previous.queries
	deltaHits := hits - previous.hits
	if deltaQueries <= 0 || deltaHits <= 0 {
		prefixCacheBaselines[channelID] = prefixCacheCounter{queries: queries, hits: hits, rate: previous.rate}
		return previous.rate, false
	}
	rate := min(1, deltaHits/deltaQueries)
	prefixCacheBaselines[channelID] = prefixCacheCounter{queries: queries, hits: hits, rate: rate}
	return rate, true
}

// IsInferenceLoadFresh reports whether a snapshot can still drive routing.
// Expired samples deliberately fail open.
func IsInferenceLoadFresh(load InferenceLoad) bool {
	if !load.OK || load.SampledAt <= 0 {
		return false
	}
	validity := time.Duration(operation_setting.GetInferenceRoutingSetting().SampleIntervalSeconds) * time.Second
	return time.Since(time.Unix(load.SampledAt, 0)) <= validity
}

// IsChannelQueueOverloaded is the single routing predicate used by both the
// random and the affinity selection paths. Disabled routing, a missing sample,
// or an expired sample all return false (fail-open).
func IsChannelQueueOverloaded(channelID int) bool {
	setting := operation_setting.GetInferenceRoutingSetting()
	if !setting.Enabled {
		return false
	}
	load, ok := GetInferenceLoad(channelID)
	if !ok || !IsInferenceLoadFresh(load) {
		return false
	}
	return load.Waiting > float64(setting.MaxWaiting)
}

// dropQueueOverloadedChannelIDs filters the memory-cache candidate ids. When
// every candidate is overloaded, the original list is returned (fail-open).
func dropQueueOverloadedChannelIDs(ids []int) []int {
	if len(ids) < 2 || !operation_setting.GetInferenceRoutingSetting().Enabled {
		return ids
	}
	kept := make([]int, 0, len(ids))
	for _, id := range ids {
		if !IsChannelQueueOverloaded(id) {
			kept = append(kept, id)
		}
	}
	if len(kept) == 0 {
		return ids
	}
	return kept
}

// abilityChannelIDs deduplicates channel ids from the DB ability rows so one
// multi-model channel only gets one sampling hint.
func abilityChannelIDs(abilities []Ability) []int {
	seen := make(map[int]struct{}, len(abilities))
	ids := make([]int, 0, len(abilities))
	for _, ability := range abilities {
		if ability.ChannelId <= 0 {
			continue
		}
		if _, ok := seen[ability.ChannelId]; ok {
			continue
		}
		seen[ability.ChannelId] = struct{}{}
		ids = append(ids, ability.ChannelId)
	}
	return ids
}

// dropQueueOverloadedAbilities filters the DB selection path the same way as
// dropQueueOverloadedChannelIDs.
func dropQueueOverloadedAbilities(abilities []Ability) []Ability {
	if len(abilities) < 2 || !operation_setting.GetInferenceRoutingSetting().Enabled {
		return abilities
	}
	kept := make([]Ability, 0, len(abilities))
	for _, ability := range abilities {
		if !IsChannelQueueOverloaded(ability.ChannelId) {
			kept = append(kept, ability)
		}
	}
	if len(kept) == 0 {
		return abilities
	}
	return kept
}

// AttachChannelQueueLoad records the billing-time queue snapshot in admin_info
// only. The public log API strips admin_info, so ordinary users never see it.
func AttachChannelQueueLoad(channelID int, other *LogOther) {
	if other == nil {
		return
	}
	load, ok := GetInferenceLoad(channelID)
	if !ok || !load.OK {
		return
	}
	other.SetAdmin("channel_queue_load", map[string]any{
		"waiting":    int(load.Waiting),
		"running":    int(load.Running),
		"overloaded": IsChannelQueueOverloaded(channelID),
	})
}
