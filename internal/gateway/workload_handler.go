package gateway

import "net/http"

type cacheStatsView struct {
	Hits      int64   `json:"hits"`
	Misses    int64   `json:"misses"`
	Evictions int64   `json:"evictions"`
	HitRate   float64 `json:"hit_rate"`
}

type workloadResponse struct {
	Mode    string         `json:"mode"`
	RPS     float64        `json:"rps"`
	HotKeys []hotKeyView   `json:"hot_keys,omitempty"`
	Cache   cacheStatsView `json:"cache"`
}

type hotKeyView struct {
	Key   string `json:"key"`
	Count int64  `json:"count"`
}

// handleWorkload reports the gateway's live view of workload state (spec
// §26 mode, §12 hot keys) and cache effectiveness (§25) — all of it
// computed from real traffic the gateway has actually proxied, not
// simulated numbers.
func (g *Gateway) handleWorkload(w http.ResponseWriter, r *http.Request) {
	status := g.workload.Status()
	stats := g.cache.Stats()

	hotKeys := make([]hotKeyView, len(status.HotKeys))
	for i, k := range status.HotKeys {
		hotKeys[i] = hotKeyView{Key: k.Key, Count: k.Count}
	}

	writeJSON(w, http.StatusOK, workloadResponse{
		Mode:    string(status.Mode),
		RPS:     status.RPS,
		HotKeys: hotKeys,
		Cache: cacheStatsView{
			Hits:      stats.Hits,
			Misses:    stats.Misses,
			Evictions: stats.Evictions,
			HitRate:   g.cache.HitRate(),
		},
	})
}
