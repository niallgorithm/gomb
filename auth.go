package gomb

import (
	"fmt"
	"path/filepath"
)

// ************************************************
// * AuthOptions
// ************************************************

// AuthOptions represents the options for authentication.
// This supports SASL/PLAIN, SASL/SCRAM, and password authentication.
// Either None or One should be set.
type AuthOptions struct {
	Basic *BasicAuth

	SASL *SASL

	TLS *TLS

	OAuth2 *OAuth2
}

// anySet returns true if any of the authentication options are set.
func (a *AuthOptions) anySet() bool {
	return a.Basic != nil || a.SASL != nil || a.TLS != nil || a.OAuth2 != nil
}

// moreThanOneSet returns true if more than one of the authentication options are set.
func (a *AuthOptions) moreThanOneSet() bool {
	count := 0
	if a.Basic != nil {
		count++
	}
	if a.SASL != nil {
		count++
	}
	if a.TLS != nil {
		count++
	}
	if a.OAuth2 != nil {
		count++
	}
	return count > 1
}

// tlsSet returns true if the TLS option is set.
func (a *AuthOptions) tlsSet() bool {
	return a.TLS != nil
}

// oauth2Set returns true if the OAuth2 option is set.
func (a *AuthOptions) oauth2Set() bool {
	return a.OAuth2 != nil
}

// Check returns an error if the authentication options are invalid.
func (a *AuthOptions) Check() error {
	if !a.anySet() {
		return nil
	}
	if a.moreThanOneSet() {
		return ErrTooManyAuths
	}
	return nil
}

// ************************************************
// * BasicAuth
// ************************************************

// BasicAuth represents the basic authentication options.
type BasicAuth struct {
	// The username to use for authentication.
	// Ignored for redis.
	Username string

	// The password to use for authentication.
	// Used for redis.
	Password string
}

// ************************************************
// * SASL
// ************************************************

type SASLMechanism string

const (
	SASLSHA256 SASLMechanism = "SASL-SHA-256"
	SASLScram  SASLMechanism = "SASL-SCRAM"
)

// SASL represents the SASL authentication options.
// For use in Kafka.
type SASL struct {
	// The mechanism to use for authentication.
	Mechanism SASLMechanism

	// The service name to use for authentication.
	ServiceName string

	// The username to use for authentication.
	Username string

	// The password to use for authentication.
	Password string
}

// Check returns an error if the SASL options are invalid.
func (s *SASL) Check() error {
	if s.Mechanism == "" {
		return &ErrAuth{
			Msg: "SASL mechanism is required",
		}
	}
	if s.ServiceName == "" {
		return &ErrAuth{
			Msg: "SASL service name is required",
		}
	}
	if s.Username == "" {
		return &ErrAuth{
			Msg: "SASL username is required",
		}
	}
	if s.Password == "" {
		return &ErrAuth{
			Msg: "SASL password is required",
		}
	}
	// check mechanism type
	switch s.Mechanism {
	case SASLSHA256, SASLScram:
		// ok
	default:
		return &ErrAuth{
			Msg: fmt.Sprintf("SASL mechanism %s is not supported", s.Mechanism),
		}
	}
	return nil
}

// ************************************************
// * TLS
// ************************************************

// TLS represents the TLS options.
// For use in Pulsar.
type TLS struct {
	// The path to the certificate file.
	CertFile string

	// The path to the key file.
	KeyFile string
}

// getCertFileName returns the certificate file name without the path.
func (t *TLS) GetCertFileName() string {
	if t.CertFile == "" {
		return ""
	}
	return filepath.Base(t.CertFile)
}

// getKeyFileName returns the key file name without the path.
func (t *TLS) GetKeyFileName() string {
	if t.KeyFile == "" {
		return ""
	}
	return filepath.Base(t.KeyFile)
}

// Check returns an error if the TLS options are invalid.
func (t *TLS) Check() error {
	if t.CertFile == "" {
		return &ErrAuth{
			Msg: "TLS certificate file is required",
		}
	}
	if t.KeyFile == "" {
		return &ErrAuth{
			Msg: "TLS key file is required",
		}
	}
	return nil
}

// ************************************************
// * OAuth2
// ************************************************

// OAuth2 represents the OAuth2 options.
// For use in Pulsar.
// Read more here: https://pulsar.apache.org/docs/2.11.x/client-libraries-go/#tls-encryption-and-authentication
type OAuth2 struct {
	// The type of OAuth2 authentication.
	Type string

	// The issuer URL.
	IssuerURL string

	// The audience.
	Audience string

	// The path to the private key file.
	PrivateKey string

	// The client ID.
	ClientID string
}

func (o *OAuth2) String() string {
	return fmt.Sprintf("type: %s, issuerUrl: %s, audience: %s, privateKey: %s, clientId: %s", o.Type, o.IssuerURL, o.Audience, o.PrivateKey, o.ClientID)
}

// ToMap converts the OAuth2 options to a map
func (o *OAuth2) ToMap() map[string]string {
	return map[string]string{
		"type":       o.Type,
		"issuerUrl":  o.IssuerURL,
		"audience":   o.Audience,
		"privateKey": o.PrivateKey,
		"clientId":   o.ClientID,
	}
}

// Check returns an error if the OAuth2 options are invalid.
func (o *OAuth2) Check() error {
	if o.Type == "" {
		return &ErrAuth{
			Msg: "OAuth2 type is required",
		}
	}
	if o.IssuerURL == "" {
		return &ErrAuth{
			Msg: "OAuth2 issuer URL is required",
		}
	}
	if o.Audience == "" {
		return &ErrAuth{
			Msg: "OAuth2 audience is required",
		}
	}
	if o.PrivateKey == "" {
		return &ErrAuth{
			Msg: "OAuth2 private key is required",
		}
	}
	if o.ClientID == "" {
		return &ErrAuth{
			Msg: "OAuth2 client ID is required",
		}
	}
	return nil
}
