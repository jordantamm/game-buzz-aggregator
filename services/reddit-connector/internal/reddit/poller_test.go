package reddit

import (
	"strings"
	"testing"
	"time"
)

func TestDeterministicULID_IsStableAcrossCalls(t *testing.T) {
	// The whole idempotency story depends on this: the same Reddit post must
	// produce the same mention_id on every run, forever.
	first := DeterministicULID("reddit", "t3_abc123")
	for i := 0; i < 100; i++ {
		if got := DeterministicULID("reddit", "t3_abc123"); got != first {
			t.Fatalf("call %d returned %q, want stable %q", i, got, first)
		}
	}
}

func TestDeterministicULID_StableAcrossTime(t *testing.T) {
	// Guards against a regression to a time.Now()-seeded timestamp prefix.
	before := DeterministicULID("reddit", "t3_stable")
	time.Sleep(5 * time.Millisecond)
	if after := DeterministicULID("reddit", "t3_stable"); after != before {
		t.Errorf("ID changed over time: %q then %q — the timestamp is not derived from the hash", before, after)
	}
}

func TestDeterministicULID_DiffersByNativeID(t *testing.T) {
	if DeterministicULID("reddit", "t3_aaa") == DeterministicULID("reddit", "t3_bbb") {
		t.Error("different native_ids collided")
	}
}

func TestDeterministicULID_DiffersBySource(t *testing.T) {
	// Future connectors must not collide with reddit on the same native_id.
	if DeterministicULID("reddit", "x1") == DeterministicULID("bluesky", "x1") {
		t.Error("different sources collided on the same native_id")
	}
}

func TestDeterministicULID_IsWellFormed(t *testing.T) {
	id := DeterministicULID("reddit", "t3_abc123")
	if len(id) != 26 {
		t.Errorf("ULID length = %d, want 26 (got %q)", len(id), id)
	}
	// Crockford base32 alphabet excludes I, L, O, and U.
	if strings.ContainsAny(id, "ILOU") {
		t.Errorf("ULID %q contains a character outside Crockford base32", id)
	}
}

func TestBackoff_GrowsAndCaps(t *testing.T) {
	base := 30 * time.Second
	max := 60 * time.Second

	first := backoff(base, max)
	if first != 45*time.Second {
		t.Errorf("backoff(30s) = %v, want 45s", first)
	}
	if capped := backoff(first, max); capped != max {
		t.Errorf("backoff(45s) = %v, want it capped at %v", capped, max)
	}
	if again := backoff(max, max); again != max {
		t.Errorf("backoff at the cap = %v, want it to stay %v", again, max)
	}
}

func TestPostKind(t *testing.T) {
	cases := map[string]string{
		"t3_abc": "post",
		"t1_def": "comment",
		"weird":  "unknown",
		"":       "unknown",
	}
	for name, want := range cases {
		if got := (postData{Name: name}).kind(); got != want {
			t.Errorf("kind(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestPostText_PrefersTitleAndBody(t *testing.T) {
	if got := (postData{Title: "T", Selftext: "B"}).text(); got != "T\n\nB" {
		t.Errorf("text() = %q, want %q", got, "T\n\nB")
	}
	if got := (postData{Title: "T"}).text(); got != "T" {
		t.Errorf("title-only text() = %q, want %q", got, "T")
	}
	if got := (postData{Body: "C"}).text(); got != "C" {
		t.Errorf("comment text() = %q, want %q", got, "C")
	}
}
