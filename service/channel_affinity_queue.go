package service

import (
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/model"
	"github.com/gin-gonic/gin"
)

// affinityStayCandidate re-validates the original affinity channel when the
// replacement chosen by the random path is overloaded too. The affinity branch
// skips these checks when it decides to bypass, so they must be repeated here
// before falling back; otherwise a disabled or group-ineligible channel could
// be selected.
//
// Boundary: returns the group the channel belongs to, which the caller writes
// back into selectGroup so billing and affinity re-anchoring stay consistent.
func affinityStayCandidate(c *gin.Context, usingGroup string, modelName string, filters []dto.ChannelFilter, channelID int) (*model.Channel, string, bool) {
	stay, err := model.CacheGetChannel(channelID)
	if err != nil || stay == nil || stay.Status != common.ChannelStatusEnabled {
		return nil, "", false
	}
	if ok, _ := model.ChannelSatisfiesFilters(stay, modelName, filters); !ok {
		return nil, "", false
	}
	if usingGroup == "auto" {
		userGroup := common.GetContextKeyString(c, constant.ContextKeyUserGroup)
		for _, group := range GetRequestAutoGroups(c, userGroup) {
			if model.IsChannelEnabledForGroupModel(group, modelName, stay.Id) {
				common.SetContextKey(c, constant.ContextKeyAutoGroup, group)
				return stay, group, true
			}
		}
		return nil, "", false
	}
	if model.IsChannelEnabledForGroupModel(usingGroup, modelName, stay.Id) {
		return stay, usingGroup, true
	}
	return nil, "", false
}
