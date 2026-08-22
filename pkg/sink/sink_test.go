package sink

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"strings"
	"testing"

	gbav1 "github.com/jordantamm/game-buzz-aggregator/gen/go/gba/v1"
)

func packFloats(values []float32) []byte {
	out := make([]byte, len(values)*4)
	for i, v := range values {
		binary.LittleEndian.PutUint32(out[i*4:], math.Float32bits(v))
	}
	return out
}

func fullVector(fill float32) []float32 {
	v := make([]float32, expectedDims)
	for i := range v {
		v[i] = fill
	}
	return v
}

func TestEmbeddingToVectorLiteral_RoundTrips(t *testing.T) {
	values := fullVector(0)
	values[0] = 0.5
	values[1] = -0.25
	values[expectedDims-1] = 1

	got, err := embeddingToVectorLiteral(packFloats(values))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(got, "[0.5,-0.25,") || !strings.HasSuffix(got, ",1]") {
		t.Errorf("literal = %.40q...%.10q, want it to start [0.5,-0.25, and end ,1]",
			got, got[len(got)-10:])
	}
	if strings.Count(got, ",") != expectedDims-1 {
		t.Errorf("got %d commas, want %d", strings.Count(got, ","), expectedDims-1)
	}
}

func TestEmbeddingToVectorLiteral_RejectsWrongDimensions(t *testing.T) {
	// A 768-dim vector from a different model must not silently reach a
	// vector(384) column.
	if _, err := embeddingToVectorLiteral(packFloats(make([]float32, 768))); err == nil {
		t.Error("expected an error for a 768-dim embedding, got nil")
	}
}

func TestEmbeddingToVectorLiteral_RejectsTruncatedBuffer(t *testing.T) {
	if _, err := embeddingToVectorLiteral([]byte{1, 2, 3}); err == nil {
		t.Error("expected an error for a buffer that is not a multiple of 4, got nil")
	}
}

func TestEmbeddingToVectorLiteral_RejectsNaN(t *testing.T) {
	values := fullVector(0)
	values[7] = float32(math.NaN())
	if _, err := embeddingToVectorLiteral(packFloats(values)); err == nil {
		t.Error("expected an error for a NaN component, got nil")
	}
}

func TestEmbeddingToVectorLiteral_RejectsInf(t *testing.T) {
	values := fullVector(0)
	values[3] = float32(math.Inf(1))
	if _, err := embeddingToVectorLiteral(packFloats(values)); err == nil {
		t.Error("expected an error for an infinite component, got nil")
	}
}

func TestSourceMetaJSON_EscapesQuotes(t *testing.T) {
	// The old implementation concatenated strings and produced invalid JSON
	// the first time a value contained a quote.
	got, err := sourceMetaJSON(map[string]string{"flair": `he said "wow"`, "subreddit": "games"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var round map[string]string
	if err := json.Unmarshal([]byte(got), &round); err != nil {
		t.Fatalf("produced invalid JSON %q: %v", got, err)
	}
	if round["flair"] != `he said "wow"` {
		t.Errorf("flair = %q, want the original value back", round["flair"])
	}
}

func TestSourceMetaJSON_EmptyMapIsEmptyObject(t *testing.T) {
	got, err := sourceMetaJSON(nil)
	if err != nil || got != "{}" {
		t.Errorf("got (%q, %v), want (\"{}\", nil)", got, err)
	}
}

func TestEngagementJSON_NilIsEmptyObject(t *testing.T) {
	got, err := engagementJSON(nil)
	if err != nil || got != "{}" {
		t.Errorf("got (%q, %v), want (\"{}\", nil)", got, err)
	}
}

func TestEngagementJSON_Values(t *testing.T) {
	got, err := engagementJSON(&gbav1.Engagement{Score: 12, ReplyCount: 3, ViewCount: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var round map[string]int64
	if err := json.Unmarshal([]byte(got), &round); err != nil {
		t.Fatalf("invalid JSON %q: %v", got, err)
	}
	if round["score"] != 12 || round["reply_count"] != 3 || round["view_count"] != 0 {
		t.Errorf("round-tripped to %v, want score=12 reply_count=3 view_count=0", round)
	}
}

func TestNullIfEmpty(t *testing.T) {
	if nullIfEmpty("") != nil {
		t.Error("empty string should map to nil (SQL NULL)")
	}
	got := nullIfEmpty("value")
	if got == nil || *got != "value" {
		t.Errorf("non-empty string should pass through, got %v", got)
	}
}
