package controller

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/setting"
	"github.com/QuantumNous/new-api/setting/config"
	"github.com/QuantumNous/new-api/setting/operation_setting"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// enableQueueRoutingForTest flips the inference routing config for the
// duration of one test. Samples injected afterwards drive both selection
// paths deterministically, so no upstream /metrics server is needed.
//
// Boundary: the routing config is process-global; ExportAllConfigs/LoadFromDB
// bracket restores every key the test overwrote.
func enableQueueRoutingForTest(t *testing.T, maxWaiting string) {
	t.Helper()
	previous := config.GlobalConfig.ExportAllConfigs()
	t.Cleanup(func() { require.NoError(t, config.GlobalConfig.LoadFromDB(previous)) })
	require.NoError(t, config.GlobalConfig.LoadFromDB(map[string]string{
		"inference_routing_setting.enabled":                        "true",
		"inference_routing_setting.max_waiting":                    maxWaiting,
		"inference_routing_setting.sample_interval":                "15",
		"inference_routing_setting.queue_refresh_cooldown_seconds": "1",
	}))
}

// setInferenceSampleForTest injects a fresh queue snapshot for one channel and
// neutralizes it on cleanup. An OK=false snapshot never drives routing, so the
// cleanup restores the pre-test behavior even if the test failed midway.
func setInferenceSampleForTest(t *testing.T, channelID int, waiting, running float64) {
	t.Helper()
	model.SetInferenceLoad(model.InferenceLoad{
		ChannelID: channelID, Waiting: waiting, Running: running,
		OK: true, SampledAt: time.Now().Unix(),
	})
	t.Cleanup(func() {
		model.SetInferenceLoad(model.InferenceLoad{ChannelID: channelID, OK: false, SampledAt: 1})
	})
}

// seedAffinityForTest binds one session to a channel the same way the pin
// retry tests do, and clears the binding on cleanup.
func seedAffinityForTest(t *testing.T, c *gin.Context, modelName string, group string, channelID int) {
	t.Helper()
	c.Request.Header.Set("X-Session", t.Name())
	_, found := service.GetPreferredChannelByAffinity(c, modelName, group)
	require.False(t, found)
	c.Set("channel_id", channelID)
	service.RecordChannelAffinity(c, channelID)
	t.Cleanup(func() { service.ClearCurrentChannelAffinityCache(c) })
	bound, found := service.GetPreferredChannelByAffinity(c, modelName, group)
	require.True(t, found)
	require.Equal(t, channelID, bound)
}

// newQueueAffinityRequest builds a Distribute-ready request whose body names
// the test model and whose session header matches the seeded binding,
// mirroring the channel pin retry test setup.
func newQueueAffinityRequest(modelName string, session string) *gin.Context {
	request := newPinRetryContext()
	request.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(fmt.Sprintf(`{"model":%q}`, modelName)))
	request.Request.Header.Set("Content-Type", "application/json")
	request.Request.Header.Set("X-Session", session)
	common.SetContextKey(request, constant.ContextKeyUsingGroup, "default")
	return request
}

// TestAffinityBypassesOverloadedChannelForQueueAwareRouting covers patch C:
// when the affinity channel's queue is over the threshold, the request goes to
// a healthy channel instead, in both prefer and strict sessions, and a
// successful request re-anchors the session onto the new channel.
func TestAffinityBypassesOverloadedChannelForQueueAwareRouting(t *testing.T) {
	require.NoError(t, i18n.Init())
	db := modelManagementDB(t, "sqlite", "")
	previousGroups := setting.UserUsableGroups2JSONString()
	previousRatios, err := common.Marshal(ratio_setting.GetGroupRatioCopy())
	require.NoError(t, err)
	affinity := operation_setting.GetChannelAffinitySetting()
	previousAffinity := *affinity
	t.Cleanup(func() {
		*affinity = previousAffinity
		require.NoError(t, setting.UpdateUserUsableGroupsByJSONString(previousGroups))
		require.NoError(t, ratio_setting.UpdateGroupRatioByJSONString(string(previousRatios)))
	})
	require.NoError(t, setting.UpdateUserUsableGroupsByJSONString(`{"default":"Default"}`))
	require.NoError(t, ratio_setting.UpdateGroupRatioByJSONString(`{"default":1}`))
	enableQueueRoutingForTest(t, "1")

	const modelName = "queue-affinity-bypass-test"
	channels := []model.Channel{
		{Id: 101, Name: "A", Type: constant.ChannelTypeOpenAI, Key: "test-only", Status: common.ChannelStatusEnabled, Models: modelName, Group: "default", Priority: common.GetPointer(int64(5)), Weight: common.GetPointer(uint(1))},
		{Id: 102, Name: "B", Type: constant.ChannelTypeOpenAI, Key: "test-only", Status: common.ChannelStatusEnabled, Models: modelName, Group: "default", Priority: common.GetPointer(int64(5)), Weight: common.GetPointer(uint(1))},
	}
	for i := range channels {
		require.NoError(t, db.Create(&channels[i]).Error)
		require.NoError(t, channels[i].AddAbilities(db))
	}

	for _, cached := range []bool{false, true} {
		for _, mode := range []string{"prefer", "strict"} {
			t.Run(fmt.Sprintf("cache=%t/mode=%s", cached, mode), func(t *testing.T) {
				common.MemoryCacheEnabled = cached
				model.InitChannelCache()
				*affinity = operation_setting.ChannelAffinitySetting{
					Enabled: true, DefaultTTLSeconds: 60, MaxEntries: 100,
					Rules: []operation_setting.ChannelAffinityRule{{
						Name: t.Name(), ModelRegex: []string{"^" + modelName + "$"}, SessionMode: mode,
						KeySources: []operation_setting.ChannelAffinityKeySource{{Type: "request_header", Key: "X-Session"}},
					}},
				}
				seed := newPinRetryContext()
				seedAffinityForTest(t, seed, modelName, "default", 101)
				setInferenceSampleForTest(t, 101, 2, 1)

				request := newQueueAffinityRequest(modelName, t.Name())
				middleware.Distribute()(request)
				assert.False(t, request.IsAborted(), "overload bypass must not fail a strict session before any request was sent")
				assert.Equal(t, 102, common.GetContextKeyInt(request, constant.ContextKeyChannelId), "the request must move to the healthy channel")
				events := service.RequestPolicy(request).Events()
				assert.True(t, slices.ContainsFunc(events, func(event service.PolicyEvent) bool {
					return event.Decision.Reason == "affinity_channel_overloaded" && event.ChannelID == 101
				}), "the bypass decision must be recorded")
				rebound, found := service.GetPreferredChannelByAffinity(request, modelName, "default")
				require.True(t, found)
				assert.Equal(t, 102, rebound, "a successful request re-anchors the session")
			})
		}
	}
}

// TestAffinityStaysOnHealthyChannel guards against over-bypassing: with no
// overload the affinity channel must keep winning.
func TestAffinityStaysOnHealthyChannel(t *testing.T) {
	require.NoError(t, i18n.Init())
	db := modelManagementDB(t, "sqlite", "")
	affinity := operation_setting.GetChannelAffinitySetting()
	previousAffinity := *affinity
	t.Cleanup(func() { *affinity = previousAffinity })
	enableQueueRoutingForTest(t, "1")

	const modelName = "queue-affinity-stay-test"
	channels := []model.Channel{
		{Id: 111, Name: "A", Type: constant.ChannelTypeOpenAI, Key: "test-only", Status: common.ChannelStatusEnabled, Models: modelName, Group: "default", Priority: common.GetPointer(int64(5)), Weight: common.GetPointer(uint(1))},
		{Id: 112, Name: "B", Type: constant.ChannelTypeOpenAI, Key: "test-only", Status: common.ChannelStatusEnabled, Models: modelName, Group: "default", Priority: common.GetPointer(int64(5)), Weight: common.GetPointer(uint(1))},
	}
	for i := range channels {
		require.NoError(t, db.Create(&channels[i]).Error)
		require.NoError(t, channels[i].AddAbilities(db))
	}

	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%t", cached), func(t *testing.T) {
			common.MemoryCacheEnabled = cached
			model.InitChannelCache()
			*affinity = operation_setting.ChannelAffinitySetting{
				Enabled: true, DefaultTTLSeconds: 60, MaxEntries: 100,
				Rules: []operation_setting.ChannelAffinityRule{{
					Name: t.Name(), ModelRegex: []string{"^" + modelName + "$"}, SessionMode: "prefer",
					KeySources: []operation_setting.ChannelAffinityKeySource{{Type: "request_header", Key: "X-Session"}},
				}},
			}
			seed := newPinRetryContext()
			seedAffinityForTest(t, seed, modelName, "default", 111)
			setInferenceSampleForTest(t, 111, 0, 1)

			request := newQueueAffinityRequest(modelName, t.Name())
			middleware.Distribute()(request)
			assert.False(t, request.IsAborted())
			assert.Equal(t, 111, common.GetContextKeyInt(request, constant.ContextKeyChannelId), "a healthy affinity channel must keep the session")
			events := service.RequestPolicy(request).Events()
			assert.False(t, slices.ContainsFunc(events, func(event service.PolicyEvent) bool {
				return event.Decision.Reason == "affinity_channel_overloaded"
			}), "no bypass event is recorded for a healthy channel")
		})
	}
}

// TestAffinityStaysWhenNoHealthierChannelExists covers patch D: when the
// randomly chosen replacement is overloaded too, the request stays on the
// original affinity channel instead of paying a KV cold start for an equally
// busy channel.
func TestAffinityStaysWhenNoHealthierChannelExists(t *testing.T) {
	require.NoError(t, i18n.Init())
	db := modelManagementDB(t, "sqlite", "")
	affinity := operation_setting.GetChannelAffinitySetting()
	previousAffinity := *affinity
	t.Cleanup(func() { *affinity = previousAffinity })
	enableQueueRoutingForTest(t, "1")

	const modelName = "queue-affinity-no-alt-test"
	channels := []model.Channel{
		{Id: 901, Name: "A", Type: constant.ChannelTypeOpenAI, Key: "test-only", Status: common.ChannelStatusEnabled, Models: modelName, Group: "default", Priority: common.GetPointer(int64(5)), Weight: common.GetPointer(uint(1))},
		// Higher priority guarantees the random path picks 902 first.
		{Id: 902, Name: "B", Type: constant.ChannelTypeOpenAI, Key: "test-only", Status: common.ChannelStatusEnabled, Models: modelName, Group: "default", Priority: common.GetPointer(int64(10)), Weight: common.GetPointer(uint(1))},
	}
	for i := range channels {
		require.NoError(t, db.Create(&channels[i]).Error)
		require.NoError(t, channels[i].AddAbilities(db))
	}

	for _, cached := range []bool{false, true} {
		t.Run(fmt.Sprintf("cache=%t", cached), func(t *testing.T) {
			common.MemoryCacheEnabled = cached
			model.InitChannelCache()
			*affinity = operation_setting.ChannelAffinitySetting{
				Enabled: true, DefaultTTLSeconds: 60, MaxEntries: 100,
				Rules: []operation_setting.ChannelAffinityRule{{
					Name: t.Name(), ModelRegex: []string{"^" + modelName + "$"}, SessionMode: "prefer",
					KeySources: []operation_setting.ChannelAffinityKeySource{{Type: "request_header", Key: "X-Session"}},
				}},
			}
			seed := newPinRetryContext()
			seedAffinityForTest(t, seed, modelName, "default", 901)
			setInferenceSampleForTest(t, 901, 2, 1)
			setInferenceSampleForTest(t, 902, 3, 1)

			request := newQueueAffinityRequest(modelName, t.Name())
			middleware.Distribute()(request)
			assert.False(t, request.IsAborted())
			assert.Equal(t, 901, common.GetContextKeyInt(request, constant.ContextKeyChannelId), "both channels overloaded means staying on the affinity channel")
			events := service.RequestPolicy(request).Events()
			bypassEvents := []service.PolicyEvent{}
			for _, event := range events {
				if event.Decision.Reason == "affinity_bypass_no_alternative" {
					bypassEvents = append(bypassEvents, event)
				}
			}
			require.Len(t, bypassEvents, 1)
			assert.Equal(t, 902, bypassEvents[0].ChannelID, "the event names the overloaded replacement that was rejected")
			bound, found := service.GetPreferredChannelByAffinity(request, modelName, "default")
			require.True(t, found)
			assert.Equal(t, 901, bound, "staying must not rewrite the binding")
		})
	}
}

// TestAttemptEventCarriesDecisionTimeQueueSnapshot covers the patch D
// observability half: channel_selected events carry the queue state as it was
// at decision time, and channels without a sample keep the old event shape.
func TestAttemptEventCarriesDecisionTimeQueueSnapshot(t *testing.T) {
	enableQueueRoutingForTest(t, "1")

	sampled := newPinRetryContext()
	setInferenceSampleForTest(t, 55, 4, 6)
	service.RequestPolicy(sampled).BeginAttempt(&model.Channel{Id: 55}, "default")
	events := service.RequestPolicy(sampled).Events()
	require.Len(t, events, 1)
	require.NotNil(t, events[0].Queue)
	assert.Equal(t, 4, events[0].Queue.Waiting)
	assert.Equal(t, 6, events[0].Queue.Running)
	assert.True(t, events[0].Queue.Overloaded)
	assert.Less(t, events[0].Queue.AgeMS, int64(5000))

	unsampled := newPinRetryContext()
	service.RequestPolicy(unsampled).BeginAttempt(&model.Channel{Id: 56}, "default")
	plainEvents := service.RequestPolicy(unsampled).Events()
	require.Len(t, plainEvents, 1)
	assert.Nil(t, plainEvents[0].Queue, "channels without a sample must not grow a queue field")
}
