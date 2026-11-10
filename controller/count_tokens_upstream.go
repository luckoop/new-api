package controller

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
)

// claudeCountTokensUpstreamPath is the Anthropic-compatible utility endpoint
// exposed by vLLM. The gateway forwards it verbatim so token counting uses the
// upstream tokenizer rather than the local estimator.
const claudeCountTokensUpstreamPath = "/v1/messages/count_tokens"

// CountClaudeTokensUpstream serves /v1/messages/count_tokens by forwarding the
// request to the already-selected vLLM channel. Any other channel type keeps
// the original local estimator, so deployments without vLLM are unchanged.
//
// Boundary: Distribute has populated the channel context keys before this
// handler runs. This handler deliberately performs no billing, usage logging,
// retries, or local token estimation; the upstream response is passed through
// byte-for-byte. Errors are returned in Claude's error shape.
//
// Errors: invalid channel address/body, proxy client construction, and upstream
// transport failures produce a 400/502 Claude error response.
func CountClaudeTokensUpstream(c *gin.Context) {
	if common.GetContextKeyInt(c, constant.ContextKeyChannelType) != constant.ChannelTypeVLLM {
		CountClaudeTokens(c)
		return
	}

	baseURL := strings.TrimRight(strings.TrimSpace(common.GetContextKeyString(c, constant.ContextKeyChannelBaseUrl)), "/")
	parsedURL, err := url.Parse(baseURL)
	if err != nil || parsedURL.Host == "" || parsedURL.User != nil || parsedURL.RawQuery != "" || parsedURL.Fragment != "" || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		writeCountTokensUpstreamError(c, http.StatusBadGateway, "api_error", "invalid inference server address")
		return
	}

	storage, err := common.GetBodyStorage(c)
	if err != nil {
		writeCountTokensUpstreamError(c, http.StatusBadRequest, "invalid_request_error", "failed to read request body")
		return
	}
	body, err := storage.Bytes()
	if err != nil {
		writeCountTokensUpstreamError(c, http.StatusBadRequest, "invalid_request_error", "failed to read request body")
		return
	}
	body, err = applyCountTokensModelMapping(body, common.GetContextKeyString(c, constant.ContextKeyChannelModelMapping))
	if err != nil {
		writeCountTokensUpstreamError(c, http.StatusBadRequest, "invalid_request_error", "failed to apply channel model mapping")
		return
	}

	// The channel settings in the context are a value copy; a missing key yields
	// a zero value, which selects the default client without a channel proxy.
	settings, _ := common.GetContextKeyType[dto.ChannelSettings](c, constant.ContextKeyChannelSetting)
	client, err := service.GetHttpClientWithProxySettings(settings.Proxy, settings)
	if err != nil {
		writeCountTokensUpstreamError(c, http.StatusBadGateway, "api_error", "invalid channel proxy")
		return
	}
	upstreamClient := *client
	upstreamClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+claudeCountTokensUpstreamPath, bytes.NewReader(body))
	if err != nil {
		writeCountTokensUpstreamError(c, http.StatusBadGateway, "api_error", "failed to build upstream request")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if key := strings.TrimSpace(common.GetContextKeyString(c, constant.ContextKeyChannelKey)); key != "" && key != "EMPTY" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for _, name := range []string{"Anthropic-Version", "Anthropic-Beta"} {
		if value := c.GetHeader(name); value != "" {
			req.Header.Set(name, value)
		}
	}

	resp, err := upstreamClient.Do(req)
	if err != nil {
		writeCountTokensUpstreamError(c, http.StatusBadGateway, "api_error", "upstream count_tokens request failed")
		return
	}
	defer resp.Body.Close()
	if contentType := resp.Header.Get("Content-Type"); contentType != "" {
		c.Header("Content-Type", contentType)
	}
	c.Status(resp.StatusCode)
	// The status and body are already being written; a copy failure cannot be
	// reported as a new JSON error, so it is only logged.
	if _, copyErr := io.Copy(c.Writer, resp.Body); copyErr != nil {
		common.SysLog(common.LogText("count_tokens upstream response copy failed: %s", copyErr.Error()))
	}
}

// applyCountTokensModelMapping rewrites only the model field with the selected
// channel's mapping. Without a mapping the original bytes are returned so the
// upstream sees the exact client payload, including fields the gateway DTOs
// do not model.
//
// Errors: returns the JSON decode/encode error; callers must not forward a
// partially rewritten payload.
func applyCountTokensModelMapping(body []byte, mappingRaw string) ([]byte, error) {
	mappingRaw = strings.TrimSpace(mappingRaw)
	if mappingRaw == "" || mappingRaw == "{}" {
		return body, nil
	}
	var modelMap map[string]string
	if err := common.Unmarshal([]byte(mappingRaw), &modelMap); err != nil {
		return nil, err
	}
	var payload map[string]any
	if err := common.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	model, _ := payload["model"].(string)
	mapped := resolveCountTokensModelMapping(modelMap, model)
	if mapped == "" || mapped == model {
		return body, nil
	}
	payload["model"] = mapped
	return common.Marshal(payload)
}

// resolveCountTokensModelMapping follows chained channel redirects and stops on
// a cycle, mirroring relay/helper.ModelMappedHelper. The reasoning-suffix
// fallback is intentionally omitted: count_tokens carries no generation
// parameters, and a wrong model name here only changes which tokenizer runs.
func resolveCountTokensModelMapping(modelMap map[string]string, model string) string {
	current := model
	visited := map[string]bool{current: true}
	for {
		next, exists := modelMap[current]
		if !exists || next == "" || visited[next] {
			return current
		}
		visited[next] = true
		current = next
	}
}

// writeCountTokensUpstreamError keeps the utility endpoint's error shape
// consistent with controller.CountClaudeTokens.
func writeCountTokensUpstreamError(c *gin.Context, status int, errorType string, message string) {
	c.JSON(status, gin.H{
		"type": "error",
		"error": gin.H{
			"type":    errorType,
			"message": common.MessageWithRequestId(message, c.GetString(common.RequestIdKey)),
		},
	})
}
