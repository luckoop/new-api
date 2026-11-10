package common

import (
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/relaykit/dto"
)

// GetAdvancedCustomPreset returns fresh defaults.
func GetAdvancedCustomPreset(channelType int) *dto.AdvancedCustomConfig {
	if channelType != constant.ChannelTypeVLLM && channelType != constant.ChannelTypeSGLang {
		return nil
	}
	config := &dto.AdvancedCustomConfig{}
	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/responses", "/v1/embeddings", "/v1/messages", dto.AdvancedCustomModelListPath} {
		config.Routes = append(config.Routes, dto.AdvancedCustomRoute{
			IncomingPath: path, UpstreamPath: path, Converter: "none",
			Auth: &dto.AdvancedCustomRouteAuth{Type: dto.AdvancedCustomAuthTypeHeader, Name: "Authorization", Value: "Bearer {api_key}"},
		})
	}
	if channelType == constant.ChannelTypeSGLang {
		for _, path := range []string{"/v1/rerank", "/rerank"} {
			config.Routes = append(config.Routes, dto.AdvancedCustomRoute{
				IncomingPath: path, UpstreamPath: "/v1/rerank", Converter: dto.AdvancedCustomConverterSGLangRerank,
			})
		}
	}
	if channelType == constant.ChannelTypeVLLM {
		// vLLM exposes a Jina-compatible /v1/rerank endpoint natively, so both
		// incoming aliases are forwarded as-is without any converter.
		for _, path := range []string{"/v1/rerank", "/rerank"} {
			config.Routes = append(config.Routes, dto.AdvancedCustomRoute{
				IncomingPath: path, UpstreamPath: "/v1/rerank", Converter: "none",
			})
		}
		// The Anthropic-compatible count_tokens utility is forwarded verbatim to
		// vLLM. Declaring it here also keeps the channel eligible under the
		// request-path filter used during channel selection.
		config.Routes = append(config.Routes, dto.AdvancedCustomRoute{
			IncomingPath: "/v1/messages/count_tokens", UpstreamPath: "/v1/messages/count_tokens", Converter: "none",
		})
	}
	return config
}
