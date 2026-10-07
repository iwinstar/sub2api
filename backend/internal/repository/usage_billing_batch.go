package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

var errUsageBillingStopped = errors.New("usage billing queue is stopped")

const (
	billingQueueCapacity    = 16384
	billingBatchSize        = 100
	billingBatchWorkers     = 8
	billingRecoveryWorkers  = 2
	billingRecoveryCapacity = 32
	billingAccountGateCount = 256
	billingBlockedTTL       = 4 * time.Second
	billingProbeInterval    = 200 * time.Millisecond
	// Queueing and recovery share this admission budget; an admitted batch
	// gets at least one lock wait plus margin. No persistent replay.
	billingProcessingBudget = 30 * time.Second
)

const (
	billingItemWaiting int32 = iota
	billingItemExecuting
	billingItemCompleted
	billingItemCancelled
)

type usageBillingBatchItem struct {
	cmd      *service.UsageBillingCommand
	state    atomic.Int32
	enqueued time.Time
	result   chan billingBatchOutcome
	release  func()
	shard    uint64
	retry    bool
	retryAt  time.Time
	unknown  bool // exclusively owned by the executor, then recovery worker
}

func (i *usageBillingBatchItem) finish(value *service.UsageBillingApplyResult, err error) {
	if i.state.CompareAndSwap(billingItemExecuting, billingItemCompleted) {
		if i.release != nil {
			i.release()
		}
		i.result <- billingBatchOutcome{value: value, err: err}
	}
}

type usageBillingBatchBackend interface {
	service.UsageBillingRepository
	apply(context.Context, *service.UsageBillingCommand, bool) (*service.UsageBillingApplyResult, error)
	ApplyBatch(context.Context, []*service.UsageBillingCommand) ([]billingBatchOutcome, error)
	probeBillingLocks(context.Context, []*service.UsageBillingCommand) ([]billingSubject, error)
}

// UsageBillingBatchRepository coalesces only already waiting commands; it never
// waits to fill a batch. Image state transitions keep their original repository.
type UsageBillingBatchRepository struct {
	base              usageBillingBatchBackend
	queues            []chan *usageBillingBatchItem
	accountGates      []chan struct{}
	permits           chan struct{}
	stopping          chan struct{}
	submitters        sync.WaitGroup
	next              atomic.Uint64
	shardMu           sync.Mutex
	accountOrdinals   map[int64]uint64
	blockedMu         sync.Mutex
	blocked           map[billingSubject]time.Time
	probes            map[billingSubject]*billingSubjectProbe
	probeSlots        chan struct{}
	probeWorkers      sync.WaitGroup
	recovery          chan []*usageBillingBatchItem
	mu                sync.RWMutex // protects submitter registration vs Stop; never held while waiting
	stopped           bool
	stopOnce          sync.Once
	workers           sync.WaitGroup
	recoverers        sync.WaitGroup
	done              chan struct{}
	transactions      atomic.Int64
	batchedOperations atomic.Int64
	batches           atomic.Int64
	maxQueueAgeNanos  atomic.Int64
}

func NewUsageBillingBatchRepository(base *usageBillingRepository) *UsageBillingBatchRepository {
	base.optimizedWrites = true
	return newUsageBillingBatchRepository(base, billingBatchWorkers, billingQueueCapacity)
}
func newUsageBillingBatchRepository(base usageBillingBatchBackend, workers, capacity int) *UsageBillingBatchRepository {
	// Sharing a gate must imply sharing a worker, including nondefault worker counts.
	gateCount := ((billingAccountGateCount + workers - 1) / workers) * workers
	r := &UsageBillingBatchRepository{base: base, accountOrdinals: make(map[int64]uint64), queues: make([]chan *usageBillingBatchItem, workers), accountGates: make([]chan struct{}, gateCount), permits: make(chan struct{}, capacity), stopping: make(chan struct{}), blocked: make(map[billingSubject]time.Time), probes: make(map[billingSubject]*billingSubjectProbe), probeSlots: make(chan struct{}, 2), recovery: make(chan []*usageBillingBatchItem, billingRecoveryCapacity), done: make(chan struct{})}
	for i := range r.accountGates {
		r.accountGates[i] = make(chan struct{}, 1)
	}
	for i := 0; i < billingRecoveryWorkers; i++ {
		r.recoverers.Add(1)
		go func() {
			defer r.recoverers.Done()
			for batch := range r.recovery {
				r.recoverBatch(batch)
			}
		}()
	}
	for i := 0; i < workers; i++ {
		r.workers.Add(1)
		r.queues[i] = make(chan *usageBillingBatchItem, capacity)
		go r.worker(r.queues[i])
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-r.done:
				return
			case <-ticker.C:
				slog.Info("billing.batch_queue", "outstanding", len(r.permits), "recovery_pending", len(r.recovery), "transactions", r.transactions.Load(), "batches", r.batches.Load(), "batched_operations", r.batchedOperations.Load(), "max_queue_age_ms", float64(r.maxQueueAgeNanos.Swap(0))/float64(time.Millisecond))
			}
		}
	}()
	return r
}

func (r *UsageBillingBatchRepository) Apply(ctx context.Context, cmd *service.UsageBillingCommand) (*service.UsageBillingApplyResult, error) {
	if cmd == nil {
		return &service.UsageBillingApplyResult{}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Own command data across caller cancellation; Normalize must run before a
	// retry can observe it, and the fingerprint must precede quantization.
	copyCmd := *cmd
	if cmd.SubscriptionID != nil {
		id := *cmd.SubscriptionID
		copyCmd.SubscriptionID = &id
	}
	copyCmd.Normalize()
	item := &usageBillingBatchItem{cmd: &copyCmd, enqueued: time.Now(), result: make(chan billingBatchOutcome, 1)}
	r.mu.RLock()
	if r.stopped {
		r.mu.RUnlock()
		return nil, errUsageBillingStopped
	}
	r.submitters.Add(1)
	r.mu.RUnlock()
	select {
	case r.permits <- struct{}{}:
	case <-ctx.Done():
		r.submitters.Done()
		return nil, ctx.Err()
	case <-r.stopping:
		r.submitters.Done()
		return nil, errUsageBillingStopped
	}
	item.release = func() { <-r.permits }
	if err := ctx.Err(); err != nil {
		item.release()
		r.submitters.Done()
		return nil, err
	}
	index := r.next.Add(1) % uint64(len(r.queues))
	if billingAccountCost(item.cmd) > 0 {
		index = r.accountShard(item.cmd.AccountID)
	}
	// The permit bounds all outstanding items, including recovery. Each shard
	// can hold that entire bound, so enqueue/requeue never waits while locked.
	item.shard = index
	r.queues[index] <- item
	r.submitters.Done()
	select {
	case result := <-item.result:
		return result.value, result.err
	case <-ctx.Done():
		if item.state.CompareAndSwap(billingItemWaiting, billingItemCancelled) {
			return nil, ctx.Err()
		}
		// Execution won the CAS. Wait for the owner to deliver its real outcome.
		result := <-item.result
		return result.value, result.err
	}
}

// Assign each account once per repository lifetime. Keep affinity across idle
// periods and recovery, without depending on gaps in database-generated IDs.
func (r *UsageBillingBatchRepository) accountShard(id int64) uint64 {
	return r.accountOrdinal(id) % uint64(len(r.queues))
}

func (r *UsageBillingBatchRepository) accountOrdinal(id int64) uint64 {
	r.shardMu.Lock()
	defer r.shardMu.Unlock()
	if index, ok := r.accountOrdinals[id]; ok {
		return index
	}
	index := uint64(len(r.accountOrdinals))
	r.accountOrdinals[id] = index
	return index
}

func (r *UsageBillingBatchRepository) worker(queue chan *usageBillingBatchItem) {
	defer r.workers.Done()
	for {
		var first *usageBillingBatchItem
		select {
		case first = <-queue:
		case <-r.stopping:
			r.submitters.Wait()
			select {
			case first = <-queue:
			default:
				if len(r.permits) == 0 {
					return
				}
				time.Sleep(5 * time.Millisecond)
				continue
			}
		}

		batch := make([]*usageBillingBatchItem, 0, billingBatchSize)
		if first.state.CompareAndSwap(billingItemWaiting, billingItemExecuting) || first.state.Load() == billingItemExecuting {
			batch = append(batch, first)
		} else {
			first.release()
		}
	collect:
		for len(batch) < billingBatchSize {
			select {
			case next := <-queue:
				if next.state.CompareAndSwap(billingItemWaiting, billingItemExecuting) || next.state.Load() == billingItemExecuting {
					batch = append(batch, next)
				} else {
					next.release()
				}
			default:
				break collect
			}
		}
		batch = liveBillingItems(batch)
		healthy := make([]*usageBillingBatchItem, 0, len(batch))
		requeued := false
		for _, item := range batch {
			if time.Now().Before(item.retryAt) {
				queue <- item
				requeued = true
				continue
			}
			if item.retry {
				select {
				case r.recovery <- []*usageBillingBatchItem{item}:
				default:
					queue <- item
					requeued = true
				}
			} else {
				healthy = append(healthy, item)
			}
		}
		// Refresh each blocked subject at most once per interval, regardless of
		// how many requests or executors reference it. Ordinary blocked work
		// stays here; only explicit single replay occupies recovery slots.
		r.refreshBlocked(healthy, true)
		batch = healthy[:0]
		for _, item := range healthy {
			if r.isBlocked(item.cmd) {
				item.retryAt = time.Now().Add(billingProbeInterval)
				queue <- item
				requeued = true
			} else {
				batch = append(batch, item)
			}
		}
		batch = liveBillingItems(batch)
		if requeued && len(batch) == 0 {
			time.Sleep(5 * time.Millisecond)
		}
		for _, item := range batch {
			age := time.Since(item.enqueued).Nanoseconds()
			for old := r.maxQueueAgeNanos.Load(); age > old; old = r.maxQueueAgeNanos.Load() {
				if r.maxQueueAgeNanos.CompareAndSwap(old, age) {
					break
				}
			}
		}
		if len(batch) == 0 {
			continue
		}
		ready, waiting, release := r.tryAccountGates(batch)
		for _, item := range waiting {
			queue <- item
		}
		if len(ready) == 0 {
			release()
			time.Sleep(5 * time.Millisecond)
			continue
		}
		err := r.execute(ready, false)
		release()
		if err != nil {
			r.retryFailedBatch(ready, err)
		}
	}
}

func liveBillingItems(batch []*usageBillingBatchItem) []*usageBillingBatchItem {
	live := batch[:0]
	for _, item := range batch {
		if item.state.Load() != billingItemExecuting {
			continue
		}
		if time.Since(item.enqueued) >= billingProcessingBudget {
			err := error(context.DeadlineExceeded)
			if item.unknown {
				err = fmt.Errorf("%w: %w", service.ErrUsageBillingOutcomeUnknown, err)
			}
			item.finish(nil, err)
		} else {
			live = append(live, item)
		}
	}
	return live
}
func finishBillingBatch(batch []*usageBillingBatchItem, err error) {
	for _, item := range batch {
		if item.unknown && !errors.Is(err, service.ErrUsageBillingOutcomeUnknown) {
			item.finish(nil, fmt.Errorf("%w: %w", service.ErrUsageBillingOutcomeUnknown, err))
		} else {
			item.finish(nil, err)
		}
	}
}

func (r *UsageBillingBatchRepository) execute(batch []*usageBillingBatchItem, recovery bool) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("billing execution panic: %v", value)
			slog.Error("billing execution panic", "error", err)
		}
	}()
	deadline := batch[0].enqueued.Add(billingProcessingBudget)
	for _, item := range batch {
		if d := item.enqueued.Add(billingProcessingBudget); d.Before(deadline) {
			deadline = d
		}
	}
	// The admission budget remains 30s. Once admitted, allow one lock wait
	// plus a small margin even if an older item is close to its deadline.
	if minimum := time.Now().Add(4 * time.Second); deadline.Before(minimum) {
		deadline = minimum
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	if recovery {
		for _, index := range r.accountGateIndexes(batch) {
			gate := r.accountGates[index]
			select {
			case gate <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			defer func(gate chan struct{}) { <-gate }(gate)
		}
	}
	for attempt := 0; attempt < 3; attempt++ {
		r.transactions.Add(1)
		if len(batch) > 1 {
			r.batches.Add(1)
			r.batchedOperations.Add(int64(len(batch)))
		}
		if len(batch) == 1 {
			var result *service.UsageBillingApplyResult
			result, err = r.base.apply(ctx, batch[0].cmd, true)
			if err == nil {
				if batch[0].unknown && result != nil && !result.Applied {
					result.ConfirmedExisting = true
				}
				r.clearBlocked(batch[0].cmd)
				batch[0].finish(result, nil)
				return nil
			}
		} else {
			cmds := make([]*service.UsageBillingCommand, len(batch))
			for i, item := range batch {
				cmds[i] = item.cmd
			}
			var results []billingBatchOutcome
			results, err = r.base.ApplyBatch(ctx, cmds)
			if errors.Is(err, service.ErrUsageBillingOutcomeUnknown) {
				// Only fresh claims may have committed. Never replay an old
				// duplicate whose retention period could expire during recovery.
				for i, result := range results {
					if !result.pendingCommit {
						batch[i].finish(result.value, result.err)
					}
				}
			}
			if err == nil {
				for i, item := range batch {
					if results[i].err == nil {
						r.clearBlocked(item.cmd)
					}
					item.finish(results[i].value, results[i].err)
				}
				return nil
			}
		}
		var pg *pq.Error
		if !errors.As(err, &pg) || pg.Code != "40P01" || attempt == 2 {
			return err
		}
		timer := time.NewTimer(time.Duration(5+time.Now().UnixNano()%11) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func billingRecoverable(err error) bool {
	var pg *pq.Error
	if errors.As(err, &pg) {
		return pg.Code == "55P03" || pg.Code == "40P01"
	}
	return errors.Is(err, service.ErrUserNotFound) || errors.Is(err, service.ErrSubscriptionNotFound) || errors.Is(err, service.ErrAccountNotFound)
}

// Failed lock transactions have rolled back before probing. Unknown commits
// retain their marker and are confirmed by the original dedup key.
func (r *UsageBillingBatchRepository) retryFailedBatch(batch []*usageBillingBatchItem, err error) {
	defer func() {
		if value := recover(); value != nil {
			finishBillingBatch(batch, fmt.Errorf("billing recovery panic: %v", value))
		}
	}()
	unknown := errors.Is(err, service.ErrUsageBillingOutcomeUnknown)
	if unknown {
		for _, item := range batch {
			item.unknown = true
		}
	}
	batch = liveBillingItems(batch)
	if len(batch) == 0 {
		return
	}
	var pg *pq.Error
	lockFailure := errors.As(err, &pg) && (pg.Code == "55P03" || pg.Code == "40P01")
	if lockFailure {
		if pg.Code == "55P03" {
			r.refreshBlocked(batch, false)
		}
	} else if !unknown && !billingRecoverable(err) {
		finishBillingBatch(batch, err)
		return
	}
	for _, item := range batch {
		// Lock failures return healthy rows to normal batching. Only commit
		// confirmation or invalid-subject isolation needs forced single replay.
		item.retry = item.unknown || !lockFailure
		item.retryAt = time.Now().Add(50 * time.Millisecond)
		r.queues[item.shard] <- item
	}
}

func (r *UsageBillingBatchRepository) recoverBatch(batch []*usageBillingBatchItem) {
	defer func() {
		if value := recover(); value != nil {
			finishBillingBatch(batch, fmt.Errorf("billing recovery panic: %v", value))
		}
	}()
	for _, item := range batch {
		one := liveBillingItems([]*usageBillingBatchItem{item})
		if len(one) == 0 {
			continue
		}
		if err := r.execute(one, true); err != nil {
			if billingRecoverable(err) && !isBillingLockError(err) {
				finishBillingBatch(one, err)
			} else {
				r.retryFailedBatch(one, err)
			}
		}
	}
}

func isBillingLockError(err error) bool {
	var pg *pq.Error
	return errors.As(err, &pg) && (pg.Code == "55P03" || pg.Code == "40P01")
}

type billingSubject struct {
	kind byte
	id   int64
}

func billingSubjects(c *service.UsageBillingCommand) []billingSubject {
	var subjects []billingSubject
	if billingAccountCost(c) > 0 {
		subjects = append(subjects, billingSubject{'a', c.AccountID})
	}
	if c.BalanceCost > 0 {
		subjects = append(subjects, billingSubject{'u', c.UserID})
	}
	if c.APIKeyQuotaCost > 0 || c.APIKeyRateLimitCost > 0 {
		subjects = append(subjects, billingSubject{'k', c.APIKeyID})
	}
	if c.SubscriptionCost > 0 && c.SubscriptionID != nil {
		subjects = append(subjects, billingSubject{'s', *c.SubscriptionID})
	}
	return subjects
}
func (r *UsageBillingBatchRepository) isBlocked(c *service.UsageBillingCommand) bool {
	r.blockedMu.Lock()
	defer r.blockedMu.Unlock()
	for _, subject := range billingSubjects(c) {
		if time.Now().Before(r.blocked[subject]) {
			return true
		}
	}
	return false
}
func (r *UsageBillingBatchRepository) clearBlocked(c *service.UsageBillingCommand) {
	r.blockedMu.Lock()
	defer r.blockedMu.Unlock()
	for _, subject := range billingSubjects(c) {
		delete(r.blocked, subject)
		if probe := r.probes[subject]; probe != nil {
			probe.invalidated = true
		}
	}
}

type billingSubjectProbe struct {
	next        time.Time
	running     bool
	invalidated bool // a successful settlement supersedes an in-flight probe
}

func (r *UsageBillingBatchRepository) refreshBlocked(batch []*usageBillingBatchItem, knownOnly bool) {
	if len(batch) == 0 {
		return
	}
	defer func() {
		if value := recover(); value != nil {
			slog.Error("billing lock probe panic", "panic", value)
		}
	}()
	now := time.Now()
	selected := make(map[billingSubject]*billingSubjectProbe)
	r.blockedMu.Lock()
	if knownOnly && len(r.blocked) == 0 {
		r.blockedMu.Unlock()
		return
	}
	for subject, probe := range r.probes {
		if !probe.running && !now.Before(probe.next) && !now.Before(r.blocked[subject]) {
			delete(r.probes, subject)
			delete(r.blocked, subject)
		}
	}
	for _, item := range batch {
		for _, subject := range billingSubjects(item.cmd) {
			if knownOnly && !now.Before(r.blocked[subject]) {
				continue
			}
			if probe := r.probes[subject]; probe != nil {
				if knownOnly && (probe.running || now.Before(probe.next)) {
					continue
				}
				// Initial post-rollback diagnosis must not be skipped because
				// refresh slots are full or a periodic snapshot is in flight.
				if !knownOnly {
					probe.invalidated = true
				}
			}
			probe := &billingSubjectProbe{next: now.Add(billingProbeInterval), running: true}
			r.probes[subject], selected[subject] = probe, probe
		}
	}
	r.blockedMu.Unlock()
	if len(selected) == 0 {
		return
	}
	if !knownOnly {
		r.runProbe(selected)
		return
	}
	// Periodic refresh alone is asynchronous and bounded.
	select {
	case r.probeSlots <- struct{}{}:
		r.probeWorkers.Add(1)
		go func() {
			defer r.probeWorkers.Done()
			defer func() { <-r.probeSlots }()
			r.runProbe(selected)
		}()
	default:
		r.blockedMu.Lock()
		for _, probe := range selected {
			probe.running = false
		}
		r.blockedMu.Unlock()
	}
}

func (r *UsageBillingBatchRepository) runProbe(selected map[billingSubject]*billingSubjectProbe) {
	defer func() {
		if value := recover(); value != nil {
			slog.Error("billing lock probe panic", "panic", value)
		}
	}()
	// Even a contained probe panic must release ownership for the next check.
	defer func() {
		r.blockedMu.Lock()
		defer r.blockedMu.Unlock()
		for _, probe := range selected {
			probe.running = false
			probe.next = time.Now().Add(billingProbeInterval)
		}
	}()
	cmds := make([]*service.UsageBillingCommand, 0, len(selected))
	for subject := range selected {
		// Probe only the selected resource, not every resource of each request.
		c := &service.UsageBillingCommand{}
		switch subject.kind {
		case 'a':
			c.AccountID, c.AccountQuotaCost, c.AccountType = subject.id, 1, service.AccountTypeAPIKey
		case 'u':
			c.UserID, c.BalanceCost = subject.id, 1
		case 'k':
			c.APIKeyID, c.APIKeyQuotaCost = subject.id, 1
		case 's':
			id := subject.id
			c.SubscriptionID, c.SubscriptionCost = &id, 1
		}
		cmds = append(cmds, c)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	blocked, err := r.base.probeBillingLocks(ctx, cmds)
	if err != nil {
		slog.Warn("billing lock probe failed", "error", err)
		return // A failed probe is not evidence against any row.
	}
	r.blockedMu.Lock()
	defer r.blockedMu.Unlock()
	for subject, probe := range selected {
		if !probe.invalidated {
			delete(r.blocked, subject)
		}
	}
	for _, subject := range blocked {
		if probe := selected[subject]; probe != nil && !probe.invalidated {
			r.blocked[subject] = time.Now().Add(billingBlockedTTL)
		}
	}
}

// Stop drains in ownership order. A slow driver COMMIT can exceed the logging
// budget; do not close the database or falsely announce a completed drain then.
func (r *UsageBillingBatchRepository) Stop() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		r.mu.Lock()
		r.stopped = true
		close(r.stopping)
		r.mu.Unlock()
		go func() { r.workers.Wait(); close(r.recovery); r.recoverers.Wait(); r.probeWorkers.Wait(); close(r.done) }()
	})
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-r.done:
		return
	case <-timer.C:
		slog.Error("billing queue shutdown still waiting; transaction outcome may be unknown")
	}
	<-r.done
}
func (r *UsageBillingBatchRepository) ReserveBatchImageBalance(ctx context.Context, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	return r.base.ReserveBatchImageBalance(ctx, cmd)
}
func (r *UsageBillingBatchRepository) CaptureBatchImageBalance(ctx context.Context, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	return r.base.CaptureBatchImageBalance(ctx, cmd)
}
func (r *UsageBillingBatchRepository) ReleaseBatchImageBalance(ctx context.Context, cmd *service.BatchImageBalanceHoldCommand) (*service.BatchImageBalanceHoldResult, error) {
	return r.base.ReleaseBatchImageBalance(ctx, cmd)
}

var _ service.UsageBillingRepository = (*UsageBillingBatchRepository)(nil)
