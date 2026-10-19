package regionsync

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
)

// 内部同步接口的鉴权头。签名覆盖时间戳、方法、路径和请求体摘要，
// 密钥只在两端配置里，不进日志、不进事件。
const (
	HeaderOrigin    = "X-Region-Sync-Origin"
	HeaderTimestamp = "X-Region-Sync-Timestamp"
	HeaderSignature = "X-Region-Sync-Signature"
)

var errBadSignature = errors.New("region sync: bad signature")

func bodyDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func signRequest(secret, origin, method, path string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	// hash.Hash 的 Write 不会返回错误。
	_, _ = io.WriteString(mac, strings.Join([]string{origin, strconv.FormatInt(ts, 10), strings.ToUpper(method), path, bodyDigest(body)}, "\n"))
	return hex.EncodeToString(mac.Sum(nil))
}

func verifyRequest(secret, wantOrigin, origin, method, path, tsHeader, sig string, body []byte, maxSkew time.Duration, now time.Time) error {
	if secret == "" || origin == "" || origin != wantOrigin {
		return errBadSignature
	}
	ts, err := strconv.ParseInt(strings.TrimSpace(tsHeader), 10, 64)
	if err != nil {
		return errBadSignature
	}
	skew := now.Sub(time.Unix(ts, 0))
	if skew < 0 {
		skew = -skew
	}
	if skew > maxSkew {
		return errBadSignature
	}
	expected := signRequest(secret, origin, method, path, ts, body)
	if !hmac.Equal([]byte(expected), []byte(strings.ToLower(strings.TrimSpace(sig)))) {
		return errBadSignature
	}
	return nil
}
