// Package export appends Postgres rows to date-partitioned JSON Lines files
// under the data directory. Layout and the at-least-once contract are in the
// README's data model section.
package export

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"trustwallet-etl/internal/store/postgres"
)

// Dataset names double as checkpoint keys.
const (
	DatasetRaw       = "raw_fetches"
	DatasetProcessed = "price_snapshots"
)

const (
	// maxRowsPerRun caps one export run. A backlog larger than this drains
	// over successive runs; at 87 rows per poll it never forms.
	maxRowsPerRun = 50_000
	// chunkRows bounds one read and write inside a run. Raw rows carry full
	// response bodies, so the chunk is what bounds memory, not the run cap.
	chunkRows = 5_000
)

// Store is the subset of the Postgres store the exporter needs.
type Store interface {
	ListRawFetchesAfter(ctx context.Context, afterID int64, limit int) ([]postgres.RawRow, error)
	ListSnapshotsAfter(ctx context.Context, afterID int64, limit int) ([]postgres.SnapshotRow, error)
	GetCheckpoint(ctx context.Context, name string) (int64, error)
	SetCheckpoint(ctx context.Context, name string, lastID int64) error
}

// Result summarises one export run. Capped means the run stopped at its cap
// with rows still unexported, which the next run picks up.
type Result struct {
	Rows   int
	Files  []string
	LastID int64
	Capped bool
}

type Exporter struct {
	store   Store
	dataDir string
	chunk   int
	cap     int
}

func New(store Store, dataDir string) *Exporter {
	return &Exporter{store: store, dataDir: dataDir, chunk: chunkRows, cap: maxRowsPerRun}
}

// record is one exported line. stem names the dataset file it belongs to,
// under the layer directory: <dir>/<stem>/dt=YYYY-MM-DD/<stem>.jsonl.
type record struct {
	id        int64
	partition time.Time
	stem      string
	line      []byte
}

// Raw dataset names, one per kind of source request, so a 2.5 MB pair
// discovery body never lands in the ticker file. One checkpoint covers both.
const (
	rawTickersStem = "ticker_24hr"
	rawPairsStem   = "exchange_info"
)

// ExportRaw appends raw fetches newer than the checkpoint to data/raw, split
// by request kind.
func (e *Exporter) ExportRaw(ctx context.Context) (Result, error) {
	return e.run(ctx, DatasetRaw, filepath.Join(e.dataDir, "raw"),
		func(ctx context.Context, after int64, limit int) ([]record, error) {
			rows, err := e.store.ListRawFetchesAfter(ctx, after, limit)
			if err != nil {
				return nil, err
			}
			out := make([]record, 0, len(rows))
			for _, r := range rows {
				line, err := json.Marshal(r)
				if err != nil {
					return nil, fmt.Errorf("encode raw_fetch %d: %w", r.ID, err)
				}
				stem := rawTickersStem
				if r.Kind == "pairs" {
					stem = rawPairsStem
				}
				out = append(out, record{id: r.ID, partition: r.FetchedAt, stem: stem, line: line})
			}
			return out, nil
		})
}

// ExportProcessed appends snapshots newer than the checkpoint to data/processed.
func (e *Exporter) ExportProcessed(ctx context.Context) (Result, error) {
	return e.run(ctx, DatasetProcessed, filepath.Join(e.dataDir, "processed"),
		func(ctx context.Context, after int64, limit int) ([]record, error) {
			rows, err := e.store.ListSnapshotsAfter(ctx, after, limit)
			if err != nil {
				return nil, err
			}
			out := make([]record, 0, len(rows))
			for _, r := range rows {
				line, err := json.Marshal(r)
				if err != nil {
					return nil, fmt.Errorf("encode snapshot %d: %w", r.ID, err)
				}
				out = append(out, record{id: r.ID, partition: r.FetchedAt, stem: "price_snapshots", line: line})
			}
			return out, nil
		})
}

// run reads unexported rows in bounded chunks. For each chunk the order is
// write, fsync, then advance the checkpoint, so a crash replays at most one
// chunk and memory stays flat however far behind the files are.
func (e *Exporter) run(ctx context.Context, dataset, dir string, list func(context.Context, int64, int) ([]record, error)) (Result, error) {
	after, err := e.store.GetCheckpoint(ctx, dataset)
	if err != nil {
		return Result{}, err
	}
	res := Result{LastID: after}
	seen := map[string]bool{}
	for res.Rows < e.cap {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		limit := min(e.chunk, e.cap-res.Rows)
		recs, err := list(ctx, after, limit)
		if err != nil || len(recs) == 0 {
			return res, err
		}
		files, err := appendPartitioned(dir, recs)
		for _, f := range files {
			if !seen[f] {
				seen[f] = true
				res.Files = append(res.Files, f)
			}
		}
		if err != nil {
			return res, err
		}
		last := recs[len(recs)-1].id
		if err := e.store.SetCheckpoint(ctx, dataset, last); err != nil {
			return res, fmt.Errorf("checkpoint %s at %d: %w", dataset, last, err)
		}
		after, res.LastID = last, last
		res.Rows += len(recs)
		if len(recs) < limit {
			return res, nil
		}
	}
	res.Capped = true
	return res, nil
}

// appendPartitioned groups records by dataset and UTC date and appends each
// group to its partition file. Records are already in id order, and groups
// preserve it.
func appendPartitioned(dir string, recs []record) ([]string, error) {
	var order []string
	groups := map[string][][]byte{}
	for _, r := range recs {
		path := filepath.Join(dir, r.stem, "dt="+r.partition.UTC().Format("2006-01-02"), r.stem+".jsonl")
		if _, ok := groups[path]; !ok {
			order = append(order, path)
		}
		groups[path] = append(groups[path], r.line)
	}
	var written []string
	for _, path := range order {
		if err := appendLines(path, groups[path]); err != nil {
			return written, err
		}
		written = append(written, path)
	}
	return written, nil
}

// appendLines opens path in append mode, writes every line, flushes and fsyncs.
// It first repairs a partial trailing line a crash may have left behind.
func appendLines(path string, lines [][]byte) (err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
	}
	if err := repairTail(path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		if cerr := f.Close(); err == nil && cerr != nil {
			err = fmt.Errorf("close %s: %w", path, cerr)
		}
	}()
	w := bufio.NewWriter(f)
	for _, l := range lines {
		if _, err := w.Write(l); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
		if err := w.WriteByte('\n'); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", path, err)
	}
	return nil
}

// repairTail truncates a partial trailing line left by a crash mid-write, so
// the file returns to its last complete record before new lines are appended.
// Safe with the checkpoint: a partial line can only belong to a batch whose
// checkpoint never advanced, so its records are re-exported whole afterwards.
func repairTail(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	size := st.Size()
	if size == 0 {
		return nil
	}
	one := make([]byte, 1)
	if _, err := f.ReadAt(one, size-1); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if one[0] == '\n' {
		return nil
	}
	keep := int64(0)
	buf := make([]byte, 64<<10)
	for end := size; end > 0 && keep == 0; {
		n := int64(len(buf))
		if n > end {
			n = end
		}
		if _, err := f.ReadAt(buf[:n], end-n); err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}
		for i := n - 1; i >= 0; i-- {
			if buf[i] == '\n' {
				keep = end - n + i + 1
				break
			}
		}
		end -= n
	}
	if err := f.Truncate(keep); err != nil {
		return fmt.Errorf("truncate %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", path, err)
	}
	return nil
}
