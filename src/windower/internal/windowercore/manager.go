package windowercore

import (
	"context"
	"log"
	"net/http"
	"sync"

	"github.com/redis/go-redis/v9"
)

// Manager ticks every configured query's Windower on each invocation and runs a single cleanup
// pass afterward, shared across all of them.
type Manager struct {
	redisClient *redis.Client
	windowers   []*Windower
}

func NewManager(redisClient *redis.Client, windowers []*Windower) *Manager {
	return &Manager{
		redisClient: redisClient,
		windowers:   windowers,
	}
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := m.Tick(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Tick advances every query's window concurrently, then runs one cleanup pass using the minimum
// in-progress window-start across all queries, so a slow/large-window query's data is never
// deleted out from under it by a faster one.
func (m *Manager) Tick(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(m.windowers))

	for _, windower := range m.windowers {
		wg.Add(1)
		go func(windower *Windower) {
			defer wg.Done()
			if err := windower.Tick(ctx); err != nil {
				errCh <- err
			}
		}(windower)
	}
	wg.Wait()
	close(errCh)

	m.cleanup(ctx)

	for err := range errCh {
		if err != nil {
			log.Printf("[Windower] tick failed: %v", err)
			return err
		}
	}
	return nil
}

func (m *Manager) cleanup(ctx context.Context) {
	removed, err := cleanupBelowMin.Run(ctx, m.redisClient, []string{windowNextKey, redisStreamKey}).Result()
	if err != nil {
		log.Printf("[Windower] cleanup failed: %v", err)
		return
	}
	if n, ok := removed.(int64); ok && n > 0 {
		log.Printf("[Windower] cleaned up %d stale event(s)", n)
	}
}
