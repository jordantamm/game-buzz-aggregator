package reddit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const (
	authURL = "https://www.reddit.com/api/v1/access_token"
	apiBase = "https://oauth.reddit.com"
)

// Client handles Reddit OAuth and listing fetches.
type Client struct {
	http        *http.Client
	clientID    string
	secret      string
	userAgent   string
	accessToken string
	tokenExpiry time.Time
}

// NewClient constructs a Reddit API client.
func NewClient(clientID, secret, userAgent string) *Client {
	return &Client{
		http:      &http.Client{Timeout: 15 * time.Second},
		clientID:  clientID,
		secret:    secret,
		userAgent: userAgent,
	}
}

func (c *Client) ensureToken(ctx context.Context) error {
	if time.Now().Before(c.tokenExpiry) {
		return nil
	}
	ctx, span := otel.Tracer("reddit-client").Start(ctx, "reddit.auth")
	defer span.End()

	body := url.Values{"grant_type": {"client_credentials"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, authURL, strings.NewReader(body.Encode()))
	if err != nil {
		return err
	}
	req.SetBasicAuth(c.clientID, c.secret)
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("reddit auth: %w", err)
	}
	defer resp.Body.Close()

	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		return fmt.Errorf("decode auth response: %w", err)
	}
	c.accessToken = tok.AccessToken
	c.tokenExpiry = time.Now().Add(time.Duration(tok.ExpiresIn-30) * time.Second)
	return nil
}

// FetchNew returns new posts/comments for the given subreddit after the cursor.
func (c *Client) FetchNew(ctx context.Context, subreddit, after string) ([]postData, string, error) {
	if err := c.ensureToken(ctx); err != nil {
		return nil, "", err
	}

	ctx, span := otel.Tracer("reddit-client").Start(ctx, "reddit.fetch_new")
	defer span.End()
	span.SetAttributes(attribute.String("subreddit", subreddit))

	endpoint := fmt.Sprintf("%s/r/%s/new?limit=100", apiBase, subreddit)
	if after != "" {
		endpoint += "&after=" + after
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Authorization", "Bearer "+c.accessToken)
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("fetch /r/%s/new: %w", subreddit, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("reddit API returned %d for /r/%s", resp.StatusCode, subreddit)
	}

	var listing listingResponse
	if err := json.NewDecoder(resp.Body).Decode(&listing); err != nil {
		return nil, "", fmt.Errorf("decode listing: %w", err)
	}

	posts := make([]postData, 0, len(listing.Data.Children))
	for _, ch := range listing.Data.Children {
		posts = append(posts, ch.Data)
	}
	return posts, listing.Data.After, nil
}
