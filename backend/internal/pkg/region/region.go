// Package region holds the process-wide multi-region identity.
//
// It is intentionally dependency-free so that the service layer (account
// scheduling, OAuth refresh ownership) and the regionsync package can share it
// without import cycles. When multi-region is disabled every helper returns the
// single-node answer, so callers need no feature checks of their own.
package region

import (
	"strings"
	"sync/atomic"
)

// Extra keys on accounts.extra that pin an account to regions.
const (
	// AccountExtraRegionPool restricts scheduling to the listed regions.
	// Value: string ("gz") or array of strings (["my","gz"]). Empty = all regions.
	AccountExtraRegionPool = "region_pool"
	// AccountExtraRefreshOwner names the only region allowed to refresh the
	// account's OAuth credentials. Empty = derived (single-region pool owner,
	// otherwise the configured default owner).
	AccountExtraRefreshOwner = "region_refresh_owner"
)

// Identity is the immutable snapshot published at startup.
type Identity struct {
	Enabled            bool
	RegionID           string
	PrimaryRegion      string
	DefaultOAuthOwner  string
	PrimaryPublicURL   string
	ProxyURLOverrides  map[int64]string
	AccountExtraLocals []string
}

var current atomic.Pointer[Identity]

// Set publishes the identity. Call once during startup (tests may call again).
func Set(id Identity) {
	id.RegionID = normalize(id.RegionID)
	id.PrimaryRegion = normalize(id.PrimaryRegion)
	id.DefaultOAuthOwner = normalize(id.DefaultOAuthOwner)
	if id.DefaultOAuthOwner == "" {
		id.DefaultOAuthOwner = id.PrimaryRegion
	}
	current.Store(&id)
}

// Reset restores single-node behaviour (tests).
func Reset() { current.Store(nil) }

// Get returns the active identity; ok=false means multi-region is disabled.
func Get() (Identity, bool) {
	p := current.Load()
	if p == nil || !p.Enabled {
		return Identity{}, false
	}
	return *p, true
}

// Enabled reports whether multi-region mode is on.
func Enabled() bool {
	_, ok := Get()
	return ok
}

// Local returns the local region id ("" when disabled).
func Local() string {
	id, ok := Get()
	if !ok {
		return ""
	}
	return id.RegionID
}

// IsPrimary is true for the management/config-primary region and for every
// single-node deployment.
func IsPrimary() bool {
	id, ok := Get()
	if !ok {
		return true
	}
	return id.RegionID == id.PrimaryRegion
}

// AccountAllowedHere reports whether an account may be scheduled in the local
// region according to accounts.extra.region_pool.
func AccountAllowedHere(extra map[string]any) bool {
	id, ok := Get()
	if !ok {
		return true
	}
	pool := regionList(extra[AccountExtraRegionPool])
	if len(pool) == 0 {
		return true
	}
	for _, r := range pool {
		if r == id.RegionID {
			return true
		}
	}
	return false
}

// RefreshOwner resolves which region owns OAuth refresh for an account.
func RefreshOwner(extra map[string]any) string {
	id, ok := Get()
	if !ok {
		return ""
	}
	if owner, _ := extra[AccountExtraRefreshOwner].(string); normalize(owner) != "" {
		return normalize(owner)
	}
	if pool := regionList(extra[AccountExtraRegionPool]); len(pool) == 1 {
		return pool[0]
	}
	return id.DefaultOAuthOwner
}

// IsRefreshOwner reports whether the local region may refresh the account's
// OAuth credentials. Always true when multi-region is disabled.
func IsRefreshOwner(extra map[string]any) bool {
	id, ok := Get()
	if !ok {
		return true
	}
	return RefreshOwner(extra) == id.RegionID
}

// ProxyURLOverride returns a region-local replacement URL for a proxy id, so
// each region can route the same synced proxy record through its own egress
// (e.g. a local mihomo port) instead of the other region's address.
func ProxyURLOverride(proxyID int64) (string, bool) {
	id, ok := Get()
	if !ok || len(id.ProxyURLOverrides) == 0 {
		return "", false
	}
	u, found := id.ProxyURLOverrides[proxyID]
	return u, found
}

func regionList(v any) []string {
	switch t := v.(type) {
	case string:
		out := []string{}
		for _, p := range strings.Split(t, ",") {
			if n := normalize(p); n != "" {
				out = append(out, n)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(t))
		for _, item := range t {
			if s, ok := item.(string); ok {
				if n := normalize(s); n != "" {
					out = append(out, n)
				}
			}
		}
		return out
	case []string:
		out := make([]string, 0, len(t))
		for _, s := range t {
			if n := normalize(s); n != "" {
				out = append(out, n)
			}
		}
		return out
	}
	return nil
}

func normalize(s string) string { return strings.ToLower(strings.TrimSpace(s)) }
