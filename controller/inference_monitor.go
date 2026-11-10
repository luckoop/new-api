package controller

import (
	"context"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"
	"github.com/bytedance/gopkg/util/gopool"
	"github.com/gin-gonic/gin"
)

const maxInferenceMetricsBytes = 4 << 20

// StartInferenceMonitor consumes sampling hints instead of running a resident
// ticker: warm every inference channel once at mount, then block on the queue.
// Each hint is handled in the shared goroutine pool; TryRefreshInferenceChannels
// already guarantees at most one in-flight sample per channel per cooldown.
func StartInferenceMonitor() {
	model.RefreshAllInferenceChannels()
	for channelID := range model.InferenceRefreshQueue() {
		id := channelID
		gopool.Go(func() {
			sampleOneInferenceChannel(id)
		})
	}
}

// sampleOneInferenceChannel keeps the last good snapshot when a /metrics fetch
// fails, so a transient upstream error does not flash zeros in the UI or make
// routing fail open on a channel that was recently known to be overloaded.
func sampleOneInferenceChannel(channelID int) {
	channel, err := model.GetChannelById(channelID, true)
	if err != nil || channel == nil {
		return
	}
	if channel.Type != constant.ChannelTypeVLLM && channel.Type != constant.ChannelTypeSGLang {
		return
	}
	load := sampleChannelMetrics(context.Background(), channel)
	if !load.OK {
		if _, exists := model.GetInferenceLoad(channelID); exists {
			return
		}
	}
	model.SetInferenceLoad(load)
}

// GetChannelInferenceLoads serves the admin queue column. Reading the list is
// also the freshness heartbeat: stale channels get a sampling hint here, so no
// background polling is needed when nobody is watching.
func GetChannelInferenceLoads(c *gin.Context) {
	model.RefreshStaleInferenceLoads()
	common.ApiSuccess(c, model.GetAllInferenceLoads())
}

// sampleChannelMetrics reuses the official status client and expfmt parser and
// fetches only /metrics. A partial or unparsable response leaves OK=false; the
// caller decides whether to keep the previous snapshot.
//
// Errors are intentionally not propagated: metrics sampling is a soft signal,
// and an unreachable /metrics must never affect request routing.
func sampleChannelMetrics(ctx context.Context, channel *model.Channel) model.InferenceLoad {
	load := model.InferenceLoad{ChannelID: channel.Id, SampledAt: time.Now().Unix()}
	baseURL, headers, client, err := prepareInferenceStatusClient(channel)
	if err != nil {
		return load
	}
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/metrics", nil)
	if err != nil {
		return load
	}
	req.Header = headers.Clone()
	req.Host = headers.Get("Host")
	resp, err := client.Do(req)
	if err != nil {
		return load
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return load
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxInferenceMetricsBytes+1))
	if err != nil || len(body) > maxInferenceMetricsBytes {
		return load
	}
	prefix := "vllm:"
	if channel.Type == constant.ChannelTypeSGLang {
		prefix = "sglang:"
	}
	metrics, err := parseInferenceMetrics(string(body), prefix)
	if err != nil || len(metrics) == 0 {
		return load
	}

	if prefix == "vllm:" {
		load.Waiting = sumInferenceMetric(metrics, "vllm:num_requests_waiting")
		load.Running = sumInferenceMetric(metrics, "vllm:num_requests_running")
		// vLLM renamed the KV gauge; keeping both avoids NaN swallowing a valid
		// value when a deployment exposes the legacy name as well.
		load.KVCacheUsage = maxNonNaN(
			maxInferenceMetric(metrics, "vllm:kv_cache_usage_perc"),
			maxInferenceMetric(metrics, "vllm:gpu_cache_usage_perc"))
		queries := sumInferenceMetric(metrics, "vllm:prefix_cache_queries_total", "vllm:prefix_cache_queries")
		hits := sumInferenceMetric(metrics, "vllm:prefix_cache_hits_total", "vllm:prefix_cache_hits")
		if queries > 0 {
			if rate, ok := model.UpdatePrefixCacheStats(channel.Id, queries, hits); ok {
				load.CacheHitRate = rate
			} else if previous, exists := model.GetInferenceLoad(channel.Id); exists {
				load.CacheHitRate = previous.CacheHitRate
			}
		}
	} else {
		load.Running = sumInferenceMetric(metrics, "sglang:num_running_reqs")
		load.Waiting = sumInferenceMetric(metrics, "sglang:num_queue_reqs")
		load.CacheHitRate = maxNonNaN(maxInferenceMetric(metrics, "sglang:cache_hit_rate"))
		load.KVCacheUsage = maxNonNaN(maxInferenceMetric(metrics, "sglang:token_usage"))
	}
	load.OK = true
	load.SampledAt = time.Now().Unix()
	return load
}

// sumInferenceMetric adds every sample with one of the given names. Missing
// metrics contribute zero; the caller decides whether zero means idle or
// unknown via load.OK.
func sumInferenceMetric(metrics []inferenceMetric, names ...string) float64 {
	total := 0.0
	for _, metric := range metrics {
		for _, name := range names {
			if metric.Name == name && !math.IsNaN(metric.Value) && !math.IsInf(metric.Value, 0) {
				total += metric.Value
				break
			}
		}
	}
	return total
}

// maxInferenceMetric returns NaN when no named sample exists so maxNonNaN can
// distinguish "absent" from a genuine zero.
func maxInferenceMetric(metrics []inferenceMetric, names ...string) float64 {
	best := math.NaN()
	for _, metric := range metrics {
		for _, name := range names {
			if metric.Name != name || math.IsNaN(metric.Value) || math.IsInf(metric.Value, 0) {
				continue
			}
			if math.IsNaN(best) || metric.Value > best {
				best = metric.Value
			}
			break
		}
	}
	return best
}

// maxNonNaN ignores absent/NaN gauges and returns zero when none are usable,
// keeping every stored value JSON-serializable.
func maxNonNaN(values ...float64) float64 {
	best := math.NaN()
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			continue
		}
		if math.IsNaN(best) || value > best {
			best = value
		}
	}
	if math.IsNaN(best) {
		return 0
	}
	return best
}
