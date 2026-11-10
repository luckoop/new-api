package operation_setting

import "github.com/QuantumNous/new-api/setting/config"

// InferenceRoutingSetting controls queue-aware channel routing. SampleInterval
// is a *validity window*, not a polling period: samples older than it make the
// router fail open. QueueRefreshCooldown bounds how often a busy channel may
// be re-sampled by request-driven triggers.
type InferenceRoutingSetting struct {
	Enabled                     bool `json:"enabled"`
	MaxWaiting                  int  `json:"max_waiting"`
	SampleIntervalSeconds       int  `json:"sample_interval"`
	QueueRefreshCooldownSeconds int  `json:"queue_refresh_cooldown_seconds"`
}

var inferenceRoutingSetting = InferenceRoutingSetting{
	Enabled:                     false,
	MaxWaiting:                  8,
	SampleIntervalSeconds:       15,
	QueueRefreshCooldownSeconds: 1,
}

func init() {
	config.GlobalConfig.Register("inference_routing_setting", &inferenceRoutingSetting)
}

// GetInferenceRoutingSetting returns a clamped copy so out-of-range values in
// the options table can never disable staleness detection or spin the sampler.
func GetInferenceRoutingSetting() InferenceRoutingSetting {
	setting := inferenceRoutingSetting
	setting.MaxWaiting = max(0, setting.MaxWaiting)
	setting.SampleIntervalSeconds = max(5, min(600, setting.SampleIntervalSeconds))
	setting.QueueRefreshCooldownSeconds = max(1, min(60, setting.QueueRefreshCooldownSeconds))
	return setting
}
