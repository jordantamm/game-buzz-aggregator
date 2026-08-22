package queries

import (
	"math"
	"testing"
)

func hits(ids ...string) []SearchHit {
	out := make([]SearchHit, 0, len(ids))
	for _, id := range ids {
		out = append(out, SearchHit{MentionID: id})
	}
	return out
}

func TestFuseRRF_DocumentInBothListsOutranksSingleListTop(t *testing.T) {
	// "b" is #2 in both lists; "a" is #1 in keyword only. Two RRF terms should
	// beat one, which is the entire point of hybrid retrieval.
	keyword := hits("a", "b", "c")
	vector := hits("d", "b", "e")

	got := fuseRRF(keyword, vector, 10)

	if len(got) == 0 {
		t.Fatal("fuseRRF returned no results")
	}
	if got[0].MentionID != "b" {
		t.Errorf("top hit = %q, want %q (found by both retrievers)", got[0].MentionID, "b")
	}
	if got[0].MatchedBy != "both" {
		t.Errorf("MatchedBy = %q, want %q", got[0].MatchedBy, "both")
	}
	if got[0].KeywordRank != 2 || got[0].VectorRank != 2 {
		t.Errorf("ranks = (%d,%d), want (2,2)", got[0].KeywordRank, got[0].VectorRank)
	}

	wantScore := 1.0/(rrfK+2) + 1.0/(rrfK+2)
	if math.Abs(got[0].Score-wantScore) > 1e-12 {
		t.Errorf("score = %v, want %v", got[0].Score, wantScore)
	}
}

func TestFuseRRF_Deduplicates(t *testing.T) {
	got := fuseRRF(hits("a", "b"), hits("b", "a"), 10)
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2 (deduplicated)", len(got))
	}
	seen := map[string]bool{}
	for _, h := range got {
		if seen[h.MentionID] {
			t.Errorf("duplicate mention_id %q in fused results", h.MentionID)
		}
		seen[h.MentionID] = true
	}
}

func TestFuseRRF_LabelsProvenance(t *testing.T) {
	got := fuseRRF(hits("kw"), hits("vec"), 10)

	byID := map[string]SearchHit{}
	for _, h := range got {
		byID[h.MentionID] = h
	}

	if byID["kw"].MatchedBy != "keyword" {
		t.Errorf("kw MatchedBy = %q, want %q", byID["kw"].MatchedBy, "keyword")
	}
	if byID["kw"].VectorRank != 0 {
		t.Errorf("kw VectorRank = %d, want 0", byID["kw"].VectorRank)
	}
	if byID["vec"].MatchedBy != "vector" {
		t.Errorf("vec MatchedBy = %q, want %q", byID["vec"].MatchedBy, "vector")
	}
	if byID["vec"].KeywordRank != 0 {
		t.Errorf("vec KeywordRank = %d, want 0", byID["vec"].KeywordRank)
	}
}

func TestFuseRRF_RespectsLimit(t *testing.T) {
	got := fuseRRF(hits("a", "b", "c", "d"), hits("e", "f"), 3)
	if len(got) != 3 {
		t.Errorf("got %d results, want 3", len(got))
	}
}

func TestFuseRRF_EmptyVectorPassIsKeywordOnly(t *testing.T) {
	// The degraded path: embedder unavailable, so only keyword hits arrive.
	// Ordering must still be the keyword ordering.
	got := fuseRRF(hits("a", "b", "c"), nil, 10)

	if len(got) != 3 {
		t.Fatalf("got %d results, want 3", len(got))
	}
	for i, want := range []string{"a", "b", "c"} {
		if got[i].MentionID != want {
			t.Errorf("position %d = %q, want %q", i, got[i].MentionID, want)
		}
		if got[i].MatchedBy != "keyword" {
			t.Errorf("position %d MatchedBy = %q, want %q", i, got[i].MatchedBy, "keyword")
		}
	}
}

func TestFuseRRF_BothEmpty(t *testing.T) {
	if got := fuseRRF(nil, nil, 10); len(got) != 0 {
		t.Errorf("got %d results, want 0", len(got))
	}
}

func TestFuseRRF_TieBreakIsDeterministic(t *testing.T) {
	// Identical single-element lists at the same rank produce equal scores for
	// distinct docs; the ID tie-break must give a stable order across runs.
	first := fuseRRF(hits("z"), hits("a"), 10)
	for i := 0; i < 20; i++ {
		got := fuseRRF(hits("z"), hits("a"), 10)
		for j := range got {
			if got[j].MentionID != first[j].MentionID {
				t.Fatalf("run %d position %d = %q, want stable %q", i, j, got[j].MentionID, first[j].MentionID)
			}
		}
	}
	if first[0].MentionID != "a" {
		t.Errorf("tie-break top = %q, want %q (lexicographically smallest)", first[0].MentionID, "a")
	}
}
