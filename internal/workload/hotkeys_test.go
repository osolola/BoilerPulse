package workload

import (
	"testing"
	"time"
)

func TestHotKeyTrackerDetectsHotKey(t *testing.T) {
	tr := NewHotKeyTracker(10*time.Second, 5)
	for i := 0; i < 5; i++ {
		tr.Record("event:mackey")
	}
	tr.Record("event:cref") // below threshold

	hot := tr.HotKeys()
	if len(hot) != 1 {
		t.Fatalf("HotKeys() = %+v, want exactly 1 hot key", hot)
	}
	if hot[0].Key != "event:mackey" || hot[0].Count != 5 {
		t.Errorf("HotKeys()[0] = %+v, want {event:mackey 5}", hot[0])
	}
}

func TestHotKeyTrackerSortedByCountDescending(t *testing.T) {
	tr := NewHotKeyTracker(10*time.Second, 2)
	for i := 0; i < 2; i++ {
		tr.Record("low")
	}
	for i := 0; i < 10; i++ {
		tr.Record("high")
	}

	hot := tr.HotKeys()
	if len(hot) != 2 {
		t.Fatalf("HotKeys() = %+v, want 2 entries", hot)
	}
	if hot[0].Key != "high" {
		t.Errorf("HotKeys()[0].Key = %q, want %q (highest count first)", hot[0].Key, "high")
	}
}

func TestHotKeyTrackerIgnoresEmptyKey(t *testing.T) {
	tr := NewHotKeyTracker(10*time.Second, 1)
	for i := 0; i < 10; i++ {
		tr.Record("")
	}
	if hot := tr.HotKeys(); len(hot) != 0 {
		t.Errorf("HotKeys() = %+v, want empty (empty key should be ignored)", hot)
	}
}

func TestHotKeyTrackerWindowExpiry(t *testing.T) {
	tr := NewHotKeyTracker(50*time.Millisecond, 3)
	for i := 0; i < 3; i++ {
		tr.Record("k")
	}
	if hot := tr.HotKeys(); len(hot) != 1 {
		t.Fatalf("HotKeys() immediately = %+v, want 1 hot key", hot)
	}

	time.Sleep(150 * time.Millisecond)
	if hot := tr.HotKeys(); len(hot) != 0 {
		t.Errorf("HotKeys() after window expiry = %+v, want empty", hot)
	}
}
