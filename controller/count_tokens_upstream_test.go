package controller

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCountTokensTestContext(body string) (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	return ctx, recorder
}

func TestCountClaudeTokensUpstream_VLLMChannel_ForwardsToUpstream(t *testing.T) {
	var gotPath, gotAuth, gotVersion, gotBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotVersion = r.Header.Get("Anthropic-Version")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"input_tokens":42}`))
	}))
	defer upstream.Close()

	ctx, recorder := newCountTokensTestContext(`{"model":"alias","messages":[{"role":"user","content":"hi"}],"tools":[{"name":"lookup"}]}`)
	ctx.Request.Header.Set("Anthropic-Version", "2023-06-01")
	common.SetContextKey(ctx, constant.ContextKeyChannelType, constant.ChannelTypeVLLM)
	common.SetContextKey(ctx, constant.ContextKeyChannelBaseUrl, upstream.URL)
	common.SetContextKey(ctx, constant.ContextKeyChannelKey, "sk-upstream")
	common.SetContextKey(ctx, constant.ContextKeyChannelModelMapping, `{"alias":"real-model"}`)
	common.SetContextKey(ctx, constant.ContextKeyChannelSetting, dto.ChannelSettings{})

	CountClaudeTokensUpstream(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, "/v1/messages/count_tokens", gotPath)
	assert.Equal(t, "Bearer sk-upstream", gotAuth)
	assert.Equal(t, "2023-06-01", gotVersion)
	assert.JSONEq(t, `{"model":"real-model","messages":[{"role":"user","content":"hi"}],"tools":[{"name":"lookup"}]}`, gotBody)
	assert.JSONEq(t, `{"input_tokens":42}`, recorder.Body.String())
}

func TestCountClaudeTokensUpstream_NonVLLMChannel_FallsBackToLocal(t *testing.T) {
	ctx, recorder := newCountTokensTestContext(`{"model":"gpt-4o","messages":[{"role":"user","content":"count this prompt"}]}`)
	common.SetContextKey(ctx, constant.ContextKeyChannelType, constant.ChannelTypeOpenAI)
	common.SetContextKey(ctx, constant.ContextKeyOriginalModel, "gpt-4o")

	CountClaudeTokensUpstream(ctx)

	require.Equal(t, http.StatusOK, recorder.Code)
	var response struct {
		InputTokens int `json:"input_tokens"`
	}
	require.NoError(t, common.Unmarshal(recorder.Body.Bytes(), &response))
	assert.Positive(t, response.InputTokens)
}

func TestCountClaudeTokensUpstream_VLLMChannel_InvalidBaseURL_ReturnsBadGateway(t *testing.T) {
	ctx, recorder := newCountTokensTestContext(`{"model":"alias","messages":[{"role":"user","content":"hi"}]}`)
	common.SetContextKey(ctx, constant.ContextKeyChannelType, constant.ChannelTypeVLLM)
	common.SetContextKey(ctx, constant.ContextKeyChannelBaseUrl, "not-a-url")

	CountClaudeTokensUpstream(ctx)

	require.Equal(t, http.StatusBadGateway, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "invalid inference server address")
}

func TestResolveCountTokensModelMapping_ChainAndCycle_StopsSafely(t *testing.T) {
	assert.Equal(t, "real-model", resolveCountTokensModelMapping(
		map[string]string{"alias": "middle", "middle": "real-model"}, "alias"))
	assert.Equal(t, "unknown", resolveCountTokensModelMapping(
		map[string]string{"alias": "middle"}, "unknown"))
	assert.Equal(t, "middle", resolveCountTokensModelMapping(
		map[string]string{"alias": "middle", "middle": "alias"}, "alias"))
}

func TestApplyCountTokensModelMapping_NoMapping_KeepsOriginalBytes(t *testing.T) {
	body := []byte(`{"model":"alias","unknown_field":123}`)
	got, err := applyCountTokensModelMapping(body, "{}")
	require.NoError(t, err)
	assert.Equal(t, body, got)

	got, err = applyCountTokensModelMapping(body, `{"alias":"real-model"}`)
	require.NoError(t, err)
	assert.JSONEq(t, `{"model":"real-model","unknown_field":123}`, string(got))
}
