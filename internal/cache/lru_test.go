package cache

import "testing"

func TestLRUGetSetRoundTrip(t *testing.T) {
	c := NewLRU(10)
	c.Set("k", []byte("v"))

	got, ok := c.Get("k")
	if !ok {
		t.Fatal("Get(k) = not found, want present")
	}
	if string(got) != "v" {
		t.Errorf("Get(k) = %q, want %q", got, "v")
	}
}

func TestLRUMissOnUnknownKey(t *testing.T) {
	c := NewLRU(10)
	if _, ok := c.Get("nope"); ok {
		t.Error("Get on empty cache = found, want not found")
	}
}

func TestLRUEvictsLeastRecentlyUsed(t *testing.T) {
	c := NewLRU(2)
	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Set("c", []byte("3")) // evicts "a" (least recently used)

	if _, ok := c.Get("a"); ok {
		t.Error("Get(a) = found, want evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("Get(b) = not found, want present")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("Get(c) = not found, want present")
	}
}

func TestLRUGetRefreshesRecency(t *testing.T) {
	c := NewLRU(2)
	c.Set("a", []byte("1"))
	c.Set("b", []byte("2"))
	c.Get("a")              // "a" is now most recently used; "b" is least
	c.Set("c", []byte("3")) // should evict "b", not "a"

	if _, ok := c.Get("a"); !ok {
		t.Error("Get(a) = not found, want present (recently accessed)")
	}
	if _, ok := c.Get("b"); ok {
		t.Error("Get(b) = found, want evicted (least recently used)")
	}
}

func TestLRUSetOverwritesExistingKey(t *testing.T) {
	c := NewLRU(10)
	c.Set("k", []byte("v1"))
	c.Set("k", []byte("v2"))

	got, _ := c.Get("k")
	if string(got) != "v2" {
		t.Errorf("Get(k) = %q, want %q", got, "v2")
	}
}

func TestLRUDelete(t *testing.T) {
	c := NewLRU(10)
	c.Set("k", []byte("v"))
	c.Delete("k")

	if _, ok := c.Get("k"); ok {
		t.Error("Get(k) after Delete = found, want not found")
	}
}

func TestLRUZeroCapacityNeverCaches(t *testing.T) {
	c := NewLRU(0)
	c.Set("k", []byte("v"))
	if _, ok := c.Get("k"); ok {
		t.Error("Get(k) with zero-capacity cache = found, want always-miss")
	}
}

func TestLRUStatsAndHitRate(t *testing.T) {
	c := NewLRU(1)
	c.Get("miss1")
	c.Set("k", []byte("v"))
	c.Get("k")                  // hit
	c.Get("k")                  // hit
	c.Get("miss2")              // miss
	c.Set("other", []byte("v")) // evicts "k"

	stats := c.Stats()
	if stats.Hits != 2 {
		t.Errorf("Hits = %d, want 2", stats.Hits)
	}
	if stats.Misses != 2 {
		t.Errorf("Misses = %d, want 2", stats.Misses)
	}
	if stats.Evictions != 1 {
		t.Errorf("Evictions = %d, want 1", stats.Evictions)
	}

	wantRate := 2.0 / 4.0
	if rate := c.HitRate(); rate != wantRate {
		t.Errorf("HitRate() = %v, want %v", rate, wantRate)
	}
}

func TestLRUHitRateWithNoLookupsIsZero(t *testing.T) {
	c := NewLRU(10)
	if rate := c.HitRate(); rate != 0 {
		t.Errorf("HitRate() with no lookups = %v, want 0", rate)
	}
}
