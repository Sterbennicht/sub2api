package regionsync

import (
	"database/sql"
	"log/slog"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/google/wire"
)

// ProviderSet 地区同步依赖。
var ProviderSet = wire.NewSet(ProvideService)

// ProvideService 创建并启动地区同步。关闭时只把本地开关写成关闭，然后什么也不做。
func ProvideService(
	cfg *config.Config,
	db *sql.DB,
	apiKeys *service.APIKeyService,
	billing *service.BillingCacheService,
	subscriptions *service.SubscriptionService,
	channels *service.ChannelService,
	errorPassthrough *service.ErrorPassthroughService,
	tlsProfiles *service.TLSFingerprintProfileService,
) *Service {
	inv := &ServiceInvalidator{
		DB: db, APIKeys: apiKeys, Billing: billing, Subscriptions: subscriptions,
		Channels: channels, ErrorPassthrough: errorPassthrough, TLSProfiles: tlsProfiles,
	}
	s := NewService(cfg.RegionSync, db, cfg.Database.DSN(), inv)
	s.Start()
	return s
}

// Middleware 非主地区的写保护/转发；主地区和单节点返回 nil。
func (s *Service) Middleware() gin.HandlerFunc {
	if !s.Enabled() {
		return nil
	}
	opts := GuardOptions{
		Enabled:                 true,
		RegionID:                s.cfg.RegionID,
		PrimaryRegion:           s.cfg.PrimaryRegion,
		PrimaryURL:              s.cfg.PrimaryPublicURL,
		LocalAdminWritePrefixes: s.cfg.LocalAdminWritePrefixes,
	}
	if s.cfg.ForwardPrimaryWrites && !s.cfg.IsPrimary() {
		fwd, err := NewPrimaryForwarder(s.cfg.PrimaryForwardURL, s.cfg.PrimaryForwardInsecureSkipVerify, s.cfg.RegionID, ForwardedClientIP)
		if err != nil {
			slog.Error("region_primary_forwarder_init_failed", "error", err)
		} else {
			opts.Forward = fwd
		}
	}
	return WriteGuard(opts)
}
