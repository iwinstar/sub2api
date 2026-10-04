package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

type ChannelMonitorV2Handler struct {
	service       *service.ChannelMonitorV2Service
	apiKeyService channelMonitorV2GroupAuthorizer
}

type channelMonitorV2GroupAuthorizer interface {
	GetAvailableGroups(ctx context.Context, userID int64) ([]service.Group, error)
}

func NewChannelMonitorV2Handler(svc *service.ChannelMonitorV2Service, apiKeyService *service.APIKeyService) *ChannelMonitorV2Handler {
	return &ChannelMonitorV2Handler{service: svc, apiKeyService: apiKeyService}
}

// channelMonitorV2IsAdmin is true when the request already passed admin auth
// (shared Dimensions/Errors handlers serve both user and admin route groups).
func channelMonitorV2IsAdmin(c *gin.Context) bool {
	role, ok := middleware.GetUserRoleFromContext(c)
	return ok && role == service.RoleAdmin
}

func (h *ChannelMonitorV2Handler) GetConfig(c *gin.Context) {
	cfg, err := h.service.GetConfig(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, cfg)
}

func (h *ChannelMonitorV2Handler) UpdateConfig(c *gin.Context) {
	var input service.ChannelMonitorV2Config
	if err := c.ShouldBindJSON(&input); err != nil {
		response.BadRequest(c, "invalid channel monitor v2 config")
		return
	}
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "user not found in context")
		return
	}
	updated, err := h.service.UpdateConfig(c.Request.Context(), input, input.Version, subject.UserID)
	if err != nil {
		if errors.Is(err, service.ErrChannelMonitorV2ConfigConflict) {
			response.Error(c, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, service.ErrChannelMonitorV2InvalidConfig) {
			response.BadRequest(c, err.Error())
			return
		}
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, updated)
}

func (h *ChannelMonitorV2Handler) Dimensions(c *gin.Context) {
	filter, ok := h.parseFilter(c)
	if !ok {
		return
	}
	admin := channelMonitorV2IsAdmin(c)
	if !h.scopeFilter(c, &filter, admin) {
		return
	}
	if !admin {
		data, err := h.service.PublicDimensionsResponse(c.Request.Context(), filter)
		writeChannelMonitorV2PublicResponse(c, data, err)
		return
	}
	result, err := h.service.DimensionsForViewer(c.Request.Context(), filter, admin)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *ChannelMonitorV2Handler) Snapshot(c *gin.Context)      { h.snapshot(c, false) }
func (h *ChannelMonitorV2Handler) AdminSnapshot(c *gin.Context) { h.snapshot(c, true) }
func (h *ChannelMonitorV2Handler) Models(c *gin.Context)        { h.models(c, false) }
func (h *ChannelMonitorV2Handler) AdminModels(c *gin.Context)   { h.models(c, true) }
func (h *ChannelMonitorV2Handler) Matrix(c *gin.Context)        { h.matrix(c, false) }
func (h *ChannelMonitorV2Handler) AdminMatrix(c *gin.Context)   { h.matrix(c, true) }
func (h *ChannelMonitorV2Handler) Users(c *gin.Context)         { h.users(c, false) }
func (h *ChannelMonitorV2Handler) AdminUsers(c *gin.Context)    { h.users(c, true) }

func (h *ChannelMonitorV2Handler) snapshot(c *gin.Context, admin bool) {
	filter, ok := h.parseFilter(c)
	if !ok {
		return
	}
	if !h.scopeFilter(c, &filter, admin) {
		return
	}
	if !admin {
		data, err := h.service.PublicSnapshotResponse(c.Request.Context(), filter)
		writeChannelMonitorV2PublicResponse(c, data, err)
		return
	}
	result, err := h.service.Snapshot(c.Request.Context(), filter, admin)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *ChannelMonitorV2Handler) models(c *gin.Context, admin bool) {
	filter, ok := h.parseFilter(c)
	if !ok {
		return
	}
	if !h.scopeFilter(c, &filter, admin) {
		return
	}
	if !admin {
		data, err := h.service.PublicModelsResponse(c.Request.Context(), filter)
		writeChannelMonitorV2PublicResponse(c, data, err)
		return
	}
	result, err := h.service.Models(c.Request.Context(), filter, admin)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *ChannelMonitorV2Handler) matrix(c *gin.Context, admin bool) {
	filter, ok := h.parseFilter(c)
	if !ok {
		return
	}
	groupBy, err := service.ParseChannelMonitorV2GroupBy(c.Query("group_by"))
	if err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if !h.scopeFilter(c, &filter, admin) {
		return
	}
	if !admin {
		data, err := h.service.PublicMatrixResponse(c.Request.Context(), filter, groupBy)
		writeChannelMonitorV2PublicResponse(c, data, err)
		return
	}
	result, err := h.service.Matrix(c.Request.Context(), filter, groupBy, admin)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func (h *ChannelMonitorV2Handler) Errors(c *gin.Context) {
	filter, ok := h.parseFilter(c)
	if !ok {
		return
	}
	admin := channelMonitorV2IsAdmin(c)
	if !h.scopeFilter(c, &filter, admin) {
		return
	}
	if !admin {
		data, err := h.service.PublicErrorsResponse(c.Request.Context(), filter)
		writeChannelMonitorV2PublicResponse(c, data, err)
		return
	}
	result, err := h.service.ErrorsForViewer(c.Request.Context(), filter, admin)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

func writeChannelMonitorV2PublicResponse(c *gin.Context, data []byte, err error) {
	if err != nil {
		if status, body := infraerrors.ToHTTP(err); status == http.StatusTooManyRequests {
			if retryAfter := body.Metadata["retry_after"]; retryAfter != "" {
				c.Header("Retry-After", retryAfter)
			}
			middleware.AbortWithError(c, status, body.Reason, body.Message)
			return
		}
		response.ErrorFrom(c, err)
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", data)
}

func (h *ChannelMonitorV2Handler) users(c *gin.Context, admin bool) {
	filter, ok := h.parseFilter(c)
	if !ok {
		return
	}
	subject, exists := middleware.GetAuthSubjectFromContext(c)
	if !exists {
		response.Error(c, http.StatusUnauthorized, "user not found in context")
		return
	}
	if !h.scopeFilter(c, &filter, admin) {
		return
	}
	targetID := subject.UserID
	lookup := strings.TrimSpace(c.Query("username"))
	if lookup != "" {
		if !admin {
			response.Error(c, http.StatusForbidden, "user lookup requires administrator access")
			return
		}
		var err error
		targetID, err = h.service.FindUserIDByUsernameOrEmail(c.Request.Context(), lookup)
		if err != nil {
			if errors.Is(err, service.ErrChannelMonitorV2AmbiguousUser) {
				response.BadRequest(c, "username or email matches multiple users")
			} else {
				response.ErrorFrom(c, err)
			}
			return
		}
	}
	result, err := h.service.Users(c.Request.Context(), filter, targetID, admin)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if lookup != "" {
		selected := []service.ChannelMonitorV2UserRow{}
		for _, row := range result.Items {
			if row.UserID != nil && *row.UserID == targetID {
				row.IsSelf = targetID == subject.UserID
				if row.DisplayLabel == "Me" {
					row.DisplayLabel = lookup
				}
				selected = append(selected, row)
				break
			}
		}
		result.Items = selected
	}
	response.Success(c, result)
}

func (h *ChannelMonitorV2Handler) scopeFilter(c *gin.Context, filter *service.ChannelMonitorV2Filter, admin bool) bool {
	if admin {
		return true
	}
	if h.apiKeyService == nil {
		response.Error(c, http.StatusInternalServerError, "channel monitor group authorization unavailable")
		return false
	}
	subject, ok := middleware.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		response.Unauthorized(c, "user not found in context")
		return false
	}
	groups, err := h.apiKeyService.GetAvailableGroups(c.Request.Context(), subject.UserID)
	if err != nil {
		response.ErrorFrom(c, err)
		return false
	}
	filter.RestrictGroups = true
	filter.AllowedGroupIDs = make([]int64, 0, len(groups))
	for i := range groups {
		filter.AllowedGroupIDs = append(filter.AllowedGroupIDs, groups[i].ID)
	}
	return true
}

func (h *ChannelMonitorV2Handler) parseFilter(c *gin.Context) (service.ChannelMonitorV2Filter, bool) {
	groups, err := parseChannelMonitorV2GroupIDs(queryList(c, "group_id"))
	if err != nil {
		response.BadRequest(c, "invalid group_id")
		return service.ChannelMonitorV2Filter{}, false
	}
	filter, err := h.service.ParseFilter(c.Query("range"), queryList(c, "platform"), queryList(c, "model"), groups)
	if err != nil {
		response.BadRequest(c, err.Error())
		return service.ChannelMonitorV2Filter{}, false
	}
	return filter, true
}

func queryList(c *gin.Context, key string) []string {
	values := c.QueryArray(key)
	result := make([]string, 0, len(values))
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			if part = strings.TrimSpace(part); part != "" {
				result = append(result, part)
			}
		}
	}
	return result
}

func parseChannelMonitorV2GroupIDs(values []string) ([]int64, error) {
	result := make([]int64, 0, len(values))
	for _, value := range values {
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil || id <= 0 {
			return nil, errors.New("invalid group id")
		}
		result = append(result, id)
	}
	return result, nil
}
