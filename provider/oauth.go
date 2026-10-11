package provider

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	pluginv1 "github.com/Silo-Server/silo-plugin-sdk/pkg/pluginproto/silo/plugin/v1"
)

const (
	authorizeURL = "https://trakt.tv/oauth/authorize"
	tokenPath    = "/oauth/token"

	// redirectAttribute is the redirect URI of the sign-in that issued these
	// tokens. Refresh sends it back when Trakt requires the same value.
	redirectAttribute = "redirect_uri"
)

// oauthFlowState is the host-encrypted PKCE verifier between the authorize
// redirect and the code exchange.
type oauthFlowState struct {
	Verifier string `json:"verifier"`
}

// InitAuthorize starts a PKCE authorization-code sign-in. Trakt's current apps
// sign users in this way and do not issue a client secret. The host supplies
// the https redirect URI it will receive the code on.
func (s *Server) InitAuthorize(ctx context.Context, req *pluginv1.WatchSyncInitAuthorizeRequest) (*pluginv1.WatchSyncInitAuthorizeResponse, error) {
	client, fault := s.appClient(req.GetCapabilityId(), req.GetProviderConfig())
	if fault != nil {
		return &pluginv1.WatchSyncInitAuthorizeResponse{Fault: fault}, nil
	}
	redirectURI := strings.TrimSpace(req.GetRedirectUri())
	state := strings.TrimSpace(req.GetState())
	if fault := validOAuthRedirect(redirectURI); fault != nil {
		return &pluginv1.WatchSyncInitAuthorizeResponse{Fault: fault}, nil
	}
	if state == "" {
		return &pluginv1.WatchSyncInitAuthorizeResponse{Fault: invalidRequestFault("Trakt sign-in state is required")}, nil
	}
	verifier, challenge, err := pkceChallenge()
	if err != nil {
		return &pluginv1.WatchSyncInitAuthorizeResponse{Fault: temporaryFault("Trakt sign-in could not be started")}, nil
	}
	query := url.Values{
		"response_type":         {"code"},
		"client_id":             {client.app.clientID},
		"redirect_uri":          {redirectURI},
		"state":                 {state},
		"code_challenge":        {challenge},
		"code_challenge_method": {"S256"},
	}
	encoded, _ := json.Marshal(oauthFlowState{Verifier: verifier})
	return &pluginv1.WatchSyncInitAuthorizeResponse{
		AuthorizationUrl: authorizeURL + "?" + query.Encode(),
		ProviderState:    encoded,
	}, nil
}

// ExchangeCode turns the authorization code into tokens. The verifier from
// InitAuthorize is the proof; a client secret is sent only when the app has one.
func (s *Server) ExchangeCode(ctx context.Context, req *pluginv1.WatchSyncExchangeCodeRequest) (*pluginv1.WatchSyncCredentialResponse, error) {
	client, fault := s.appClient(req.GetCapabilityId(), req.GetProviderConfig())
	if fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	redirectURI := strings.TrimSpace(req.GetRedirectUri())
	code := strings.TrimSpace(req.GetAuthorizationCode())
	if fault := validOAuthRedirect(redirectURI); fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	if code == "" {
		return &pluginv1.WatchSyncCredentialResponse{Fault: invalidRequestFault("Trakt authorization code is required")}, nil
	}
	var state oauthFlowState
	if err := json.Unmarshal(req.GetProviderState(), &state); err != nil || !validVerifier(state.Verifier) {
		return &pluginv1.WatchSyncCredentialResponse{Fault: invalidRequestFault("Trakt sign-in state is invalid")}, nil
	}
	ctx, cancel, _ := s.window(ctx)
	defer cancel()
	fields := withClientSecret(client.app, map[string]string{
		"code":          code,
		"client_id":     client.app.clientID,
		"redirect_uri":  redirectURI,
		"grant_type":    "authorization_code",
		"code_verifier": state.Verifier,
	})
	var token traktToken
	if _, _, fault := client.do(ctx, http.MethodPost, tokenPath, nil, fields, &token); fault != nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: fault}, nil
	}
	credentials := s.credentials(token, "")
	if credentials == nil {
		return &pluginv1.WatchSyncCredentialResponse{Fault: temporaryFault("Trakt returned no access token")}, nil
	}
	credentials.SecretAttributes = map[string]string{redirectAttribute: redirectURI}
	return &pluginv1.WatchSyncCredentialResponse{Credentials: credentials}, nil
}

func validOAuthRedirect(redirectURI string) *pluginv1.WatchSyncFault {
	parsed, err := url.Parse(redirectURI)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
		return invalidRequestFault("Trakt redirect URI must be an https address")
	}
	return nil
}

func validVerifier(verifier string) bool {
	if len(verifier) < 43 || len(verifier) > 128 {
		return false
	}
	for _, r := range verifier {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', strings.ContainsRune("-._~", r):
		default:
			return false
		}
	}
	return true
}

// pkceChallenge returns a verifier and its S256 challenge. The verifier is
// 32 random bytes, base64url without padding, which is 43 characters.
func pkceChallenge() (verifier, challenge string, err error) {
	raw := make([]byte, 32)
	if _, err = rand.Read(raw); err != nil {
		return "", "", err
	}
	verifier = base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(verifier))
	challenge = base64.RawURLEncoding.EncodeToString(sum[:])
	return verifier, challenge, nil
}
