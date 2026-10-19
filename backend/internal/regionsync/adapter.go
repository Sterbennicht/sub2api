package regionsync

import (
	"context"
	"database/sql"
	"errors"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// ServiceInvalidator 把同步落库后的失效请求转给现有缓存服务（与后台管理改动走同一套失效逻辑）。
type ServiceInvalidator struct {
	DB               *sql.DB
	APIKeys          *service.APIKeyService
	Billing          *service.BillingCacheService
	Subscriptions    *service.SubscriptionService
	Channels         *service.ChannelService
	ErrorPassthrough *service.ErrorPassthroughService
	TLSProfiles      *service.TLSFingerprintProfileService
}

func (s *ServiceInvalidator) InvalidateUserAuth(ctx context.Context, userID int64) {
	if s.APIKeys != nil {
		s.APIKeys.InvalidateAuthCacheByUserID(ctx, userID)
	}
}

func (s *ServiceInvalidator) InvalidateUserBalance(ctx context.Context, userID int64) {
	if s.Billing != nil {
		_ = s.Billing.InvalidateUserBalance(ctx, userID)
	}
}

func (s *ServiceInvalidator) InvalidateAPIKey(ctx context.Context, keyID int64) {
	if s.Billing != nil {
		_ = s.Billing.InvalidateAPIKeyRateLimit(ctx, keyID)
	}
	if s.APIKeys == nil || s.DB == nil {
		return
	}
	var key string
	err := s.DB.QueryRowContext(ctx, `SELECT key FROM api_keys WHERE id = $1`, keyID).Scan(&key)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return
	}
	if key != "" {
		s.APIKeys.InvalidateAuthCacheByKey(ctx, key)
	}
}

func (s *ServiceInvalidator) InvalidateGroup(ctx context.Context, groupID int64) {
	if s.APIKeys != nil {
		s.APIKeys.InvalidateAuthCacheByGroupID(ctx, groupID)
	}
}

func (s *ServiceInvalidator) InvalidateSubscription(ctx context.Context, userID, groupID int64) {
	if s.Billing != nil {
		_ = s.Billing.InvalidateSubscription(ctx, userID, groupID)
	}
	if s.Subscriptions != nil {
		s.Subscriptions.InvalidateSubCache(userID, groupID)
	}
}

func (s *ServiceInvalidator) InvalidateChannels(context.Context) {
	if s.Channels != nil {
		s.Channels.InvalidateCache()
	}
}

func (s *ServiceInvalidator) InvalidateErrorPassthrough(ctx context.Context) {
	if s.ErrorPassthrough != nil {
		s.ErrorPassthrough.ReloadFromStore(ctx)
	}
}

func (s *ServiceInvalidator) InvalidateTLSProfiles(ctx context.Context) {
	if s.TLSProfiles != nil {
		s.TLSProfiles.ReloadFromStore(ctx)
	}
}
