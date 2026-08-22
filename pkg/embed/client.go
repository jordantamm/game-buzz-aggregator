// Package embed provides a client for the enricher's embedding HTTP endpoint.
//
// Query-time embeddings have to come from the exact same model that produced
// the stored mention embeddings, otherwise cosine distance is meaningless. That
// model (sentence-transformers all-MiniLM-L6-v2) is loaded by the Python
// enricher, so the Go services call it over HTTP rather than trying to run a
// second copy of the model. The enricher serves this on /embed.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
)

// Dims is the dimensionality of the all-MiniLM-L6-v2 embedding space.
const Dims = 384

// Client calls the enricher's embedding endpoint.
type Client struct {
	baseURL string
	http    *http.Client
}

// NewClient returns a client for the embedder at baseURL (e.g. "http://enricher:8000").
// timeout bounds each request; callers should also pass a context deadline.
func NewClient(baseURL string, timeout time.Duration) *Client {
	return &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		http:    &http.Client{Timeout: timeout},
	}
}

type embedRequest struct {
	Text string `json:"text"`
}

type embedResponse struct {
	Embedding []float32 `json:"embedding"`
	Model     string    `json:"model"`
}

// Embed returns the embedding vector for text.
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	ctx, span := otel.Tracer("pkg/embed").Start(ctx, "embed.Embed")
	defer span.End()
	span.SetAttributes(attribute.Int("embed.text_length", len(text)))

	body, err := json.Marshal(embedRequest{Text: text})
	if err != nil {
		return nil, fmt.Errorf("marshal embed request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/embed", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build embed request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Propagate the trace so the embedder's span joins this trace.
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(req.Header))

	resp, err := c.http.Do(req)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("call embedder: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("embedder returned %s", resp.Status)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	var out embedResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode embed response: %w", err)
	}
	if len(out.Embedding) != Dims {
		return nil, fmt.Errorf("embedder returned %d dims, want %d", len(out.Embedding), Dims)
	}
	return out.Embedding, nil
}

// Vector renders a float32 slice as a pgvector literal, e.g. "[0.1,0.2]".
// pgx has no native pgvector type without the pgvector-go extension, so the
// query casts this text form with ::vector.
func Vector(v []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%g", f)
	}
	b.WriteByte(']')
	return b.String()
}
