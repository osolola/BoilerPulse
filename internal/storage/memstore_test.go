package storage

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMemStorePutGet(t *testing.T) {
	m := NewMemStore()

	if err := m.Put("event:123", []byte(`{"title":"Purdue Basketball"}`), ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	entry, err := m.Get("event:123")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if string(entry.Value) != `{"title":"Purdue Basketball"}` {
		t.Errorf("Value = %q, want %q", entry.Value, `{"title":"Purdue Basketball"}`)
	}
	if entry.Consistency != ConsistencyEventual {
		t.Errorf("Consistency = %q, want %q", entry.Consistency, ConsistencyEventual)
	}
}

func TestMemStoreGetMissing(t *testing.T) {
	m := NewMemStore()

	if _, err := m.Get("does-not-exist"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get on missing key = %v, want ErrKeyNotFound", err)
	}
}

func TestMemStoreOverwrite(t *testing.T) {
	m := NewMemStore()

	mustPut(t, m, "k", "v1")
	mustPut(t, m, "k", "v2")

	entry, err := m.Get("k")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if string(entry.Value) != "v2" {
		t.Errorf("Value = %q, want %q", entry.Value, "v2")
	}
}

func TestMemStoreDelete(t *testing.T) {
	m := NewMemStore()
	mustPut(t, m, "k", "v")

	if err := m.Delete("k"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if _, err := m.Get("k"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get after Delete = %v, want ErrKeyNotFound", err)
	}
}

func TestMemStoreDeleteMissing(t *testing.T) {
	m := NewMemStore()

	if err := m.Delete("does-not-exist"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Delete on missing key = %v, want ErrKeyNotFound", err)
	}
}

func TestMemStoreDeleteTwiceIsTombstoned(t *testing.T) {
	m := NewMemStore()
	mustPut(t, m, "k", "v")

	if err := m.Delete("k"); err != nil {
		t.Fatalf("first Delete returned error: %v", err)
	}
	if err := m.Delete("k"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("second Delete = %v, want ErrKeyNotFound", err)
	}
}

func TestMemStoreTTLExpiry(t *testing.T) {
	m := NewMemStore()
	current := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return current }

	if err := m.Put("k", []byte("v"), ConsistencyEventual, time.Minute); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	// Still within TTL.
	current = current.Add(30 * time.Second)
	if _, err := m.Get("k"); err != nil {
		t.Fatalf("Get before expiry returned error: %v", err)
	}

	// Past TTL.
	current = current.Add(time.Minute)
	if _, err := m.Get("k"); !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("Get after expiry = %v, want ErrKeyNotFound", err)
	}
}

func TestMemStoreNoTTLNeverExpires(t *testing.T) {
	m := NewMemStore()
	current := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return current }

	mustPut(t, m, "k", "v")

	current = current.Add(24 * time.Hour)
	if _, err := m.Get("k"); err != nil {
		t.Errorf("Get with no TTL after 24h returned error: %v", err)
	}
}

func TestMemStoreConcurrentAccess(t *testing.T) {
	m := NewMemStore()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			_ = m.Put("k", []byte("v"), ConsistencyEventual, 0)
		}(i)
		go func(i int) {
			defer wg.Done()
			_, _ = m.Get("k")
		}(i)
	}
	wg.Wait()
}

func TestMemStoreScanFiltersByPrefixAndSkipsTombstonesAndExpired(t *testing.T) {
	m := NewMemStore()
	mustPut(t, m, "event:1", "a")
	mustPut(t, m, "event:2", "b")
	mustPut(t, m, "other:1", "c")

	if err := m.Put("event:deleted", []byte("d"), ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := m.Delete("event:deleted"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	current := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return current }
	if err := m.Put("event:expired", []byte("e"), ConsistencyEventual, time.Minute); err != nil {
		t.Fatalf("Put: %v", err)
	}
	current = current.Add(2 * time.Minute)

	got, err := m.Scan("event:")
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Scan returned %d entries, want 2: %+v", len(got), got)
	}
	if string(got["event:1"].Value) != "a" || string(got["event:2"].Value) != "b" {
		t.Errorf("Scan results = %+v, want event:1=a event:2=b", got)
	}
}

func mustPut(t *testing.T, m *MemStore, key, value string) {
	t.Helper()
	if err := m.Put(key, []byte(value), ConsistencyEventual, 0); err != nil {
		t.Fatalf("Put(%q, %q) returned error: %v", key, value, err)
	}
}
