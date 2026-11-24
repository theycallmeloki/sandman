package main

// Optional HTTP Basic Auth for the daemon's HTTP surface — the JSON API,
// the MCP transport, and the embedded dashboard. It is disabled by
// default (empty user and password) so a trusted-LAN install keeps the
// historical behaviour; setting both enables it for every HTTP request.
//
// The text protocol on the same port and the worker's exec endpoint are
// separate protocols and are deliberately not covered here.
//
// Credentials are never logged: startup errors name the missing flag or
// the unreadable file, never a value.

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
)

// authConfig is the daemon's HTTP Basic Auth credential pair. The zero
// value disables auth.
type authConfig struct {
	User     string
	Password string
}

// enabled reports whether auth is configured. A partially configured
// pair also counts as enabled (and therefore fail-closed) — startup
// rejects that state, but the middleware must never silently allow
// traffic if a caller builds one directly.
func (a authConfig) enabled() bool {
	return a.User != "" || a.Password != ""
}

// resolveAuthConfig validates credentials supplied from flags and/or a
// password file. Both fields must be set or neither; specifying both a
// literal password and a password file is ambiguous and rejected. The
// file's trailing newline is trimmed but any other whitespace is kept
// (a password may legitimately contain spaces).
func resolveAuthConfig(user, pass, passFile string) (authConfig, error) {
	if pass != "" && passFile != "" {
		return authConfig{}, errors.New("set only one of -auth-password and -auth-password-file")
	}
	if passFile != "" {
		b, err := os.ReadFile(passFile)
		if err != nil {
			return authConfig{}, fmt.Errorf("read password file %q: %w", passFile, err)
		}
		pass = strings.TrimRight(string(b), "\r\n")
	}
	cfg := authConfig{User: user, Password: pass}
	if (cfg.User == "") != (cfg.Password == "") {
		return authConfig{}, errors.New("HTTP Basic Auth needs both -auth-user and a password (-auth-password or -auth-password-file); set neither to disable")
	}
	return cfg, nil
}

// authFromEnv fills unset flag values from the SANDMAN_AUTH_* variables.
// An explicit flag always wins; the fallback is per field, so a user may
// pass -auth-user while the password comes from the environment.
func authFromEnv(user, pass, passFile string) (authConfig, error) {
	if user == "" {
		user = os.Getenv("SANDMAN_AUTH_USER")
	}
	if pass == "" {
		pass = os.Getenv("SANDMAN_AUTH_PASSWORD")
	}
	if passFile == "" {
		passFile = os.Getenv("SANDMAN_AUTH_PASSWORD_FILE")
	}
	return resolveAuthConfig(user, pass, passFile)
}

// sameCredential compares two strings in constant time. Comparing fixed
// SHA-256 digests (rather than the raw bytes) keeps the comparison
// constant-time even when the lengths differ, which subtle's raw
// comparison cannot hide.
func sameCredential(a, b string) bool {
	ah := sha256.Sum256([]byte(a))
	bh := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(ah[:], bh[:]) == 1
}

// apply adds the configured Basic Auth credentials to an outgoing request.
// It is shared by the worker's control-plane registration/heartbeat path;
// an empty pair leaves the request unauthenticated.
func (a authConfig) apply(req *http.Request) {
	if a.User != "" && a.Password != "" {
		req.SetBasicAuth(a.User, a.Password)
	}
}

// basicAuth wraps next with HTTP Basic Auth. Both the user and the
// password are always compared (no short-circuit) so response timing does
// not reveal which half was wrong, and the 401 body uses the API's usual
// JSON error shape so non-auth failures keep their existing contract.
func basicAuth(cfg authConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		userOK := sameCredential(user, cfg.User)
		passOK := sameCredential(pass, cfg.Password)
		if !ok || !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="sandman"`)
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}
