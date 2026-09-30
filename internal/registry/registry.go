// Package registry loads the asset reference table from a checked-in snapshot
// of Trust Wallet's public assets registry. It is the dimension side of the
// model: price snapshots carry a ticker symbol, the registry says which asset
// that symbol belongs to on which chain.
package registry

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Asset is one row of the registry.
type Asset struct {
	AssetID         string
	CoinID          int
	Chain           string
	ContractAddress string
	Symbol          string
	Name            string
	Decimals        int
	TokenType       string
}

var requiredColumns = []string{"asset_id", "coin_id", "chain", "contract_address", "symbol", "name", "decimals", "token_type"}

// Load reads and validates the registry CSV at path.
func Load(path string) ([]Asset, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open registry: %w", err)
	}
	defer f.Close()
	assets, err := Parse(f)
	if err != nil {
		return nil, fmt.Errorf("registry %s: %w", path, err)
	}
	return assets, nil
}

// Parse reads the registry from r. The header must contain every required
// column; extra columns are ignored so the snapshot can carry more fields later.
func Parse(r io.Reader) ([]Asset, error) {
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true

	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}
	col := map[string]int{}
	for i, h := range header {
		col[strings.TrimSpace(h)] = i
	}
	for _, c := range requiredColumns {
		if _, ok := col[c]; !ok {
			return nil, fmt.Errorf("missing column %q", c)
		}
	}

	var (
		assets []Asset
		seen   = map[string]int{}
		errs   []error
	)
	for line := 2; ; line++ {
		rec, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", line, err)
		}
		a, err := parseRecord(rec, col)
		if err != nil {
			errs = append(errs, fmt.Errorf("line %d: %w", line, err))
			continue
		}
		if prev, dup := seen[a.AssetID]; dup {
			errs = append(errs, fmt.Errorf("line %d: duplicate asset_id %q (first seen line %d)", line, a.AssetID, prev))
			continue
		}
		seen[a.AssetID] = line
		assets = append(assets, a)
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	if len(assets) == 0 {
		return nil, errors.New("registry is empty")
	}
	return assets, nil
}

func parseRecord(rec []string, col map[string]int) (Asset, error) {
	get := func(name string) string {
		i := col[name]
		if i >= len(rec) {
			return ""
		}
		return strings.TrimSpace(rec[i])
	}
	a := Asset{
		AssetID:         get("asset_id"),
		Chain:           get("chain"),
		ContractAddress: get("contract_address"),
		Symbol:          get("symbol"),
		Name:            get("name"),
		TokenType:       get("token_type"),
	}
	if a.AssetID == "" {
		return a, errors.New("empty asset_id")
	}
	if a.Symbol == "" {
		return a, fmt.Errorf("asset %s: empty symbol", a.AssetID)
	}
	var err error
	if a.CoinID, err = strconv.Atoi(get("coin_id")); err != nil {
		return a, fmt.Errorf("coin_id: %w", err)
	}
	if a.Decimals, err = strconv.Atoi(get("decimals")); err != nil {
		return a, fmt.Errorf("decimals: %w", err)
	}
	return a, nil
}

// Symbols returns the distinct upper-cased symbols in sorted order. Price
// sources key on upper-case tickers; the registry keeps the issuer's casing.
func Symbols(assets []Asset) []string {
	set := map[string]struct{}{}
	for _, a := range assets {
		set[strings.ToUpper(a.Symbol)] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// SymbolMap is the reviewed link from a source ticker symbol to the registry
// asset it prices. A symbol listed without an asset id was reviewed and
// excluded: the source's pair with that ticker is a different project, and
// every excluded row carries a note saying which. A symbol absent from the map
// is not priced at all, so an unreviewed pair can never publish a price.
type SymbolMap struct {
	Verified map[string]string // symbol to asset_id
	Excluded []string          // reviewed, and not the registry's asset
}

// LoadSymbolMap reads symbol_map.csv (symbol, asset_id, note) and checks every
// asset id against the registry, including that the asset's own symbol matches,
// so a typo cannot bind a price to the wrong asset.
func LoadSymbolMap(path string, assets []Asset) (SymbolMap, error) {
	f, err := os.Open(path)
	if err != nil {
		return SymbolMap{}, fmt.Errorf("open symbol map: %w", err)
	}
	defer f.Close()
	m, err := ParseSymbolMap(f, assets)
	if err != nil {
		return SymbolMap{}, fmt.Errorf("symbol map %s: %w", path, err)
	}
	return m, nil
}

// ParseSymbolMap reads the map from r against the registry it must agree with.
func ParseSymbolMap(r io.Reader, assets []Asset) (SymbolMap, error) {
	byID := make(map[string]Asset, len(assets))
	for _, a := range assets {
		byID[a.AssetID] = a
	}
	reader := csv.NewReader(r)
	reader.TrimLeadingSpace = true
	header, err := reader.Read()
	if err != nil {
		return SymbolMap{}, fmt.Errorf("read header: %w", err)
	}
	if len(header) < 3 || strings.TrimSpace(header[0]) != "symbol" || strings.TrimSpace(header[1]) != "asset_id" || strings.TrimSpace(header[2]) != "note" {
		return SymbolMap{}, errors.New(`header must be "symbol,asset_id,note"`)
	}
	m := SymbolMap{Verified: map[string]string{}}
	seen := map[string]bool{}
	var errs []error
	for line := 2; ; line++ {
		rec, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return SymbolMap{}, fmt.Errorf("line %d: %w", line, err)
		}
		sym, id, note := strings.ToUpper(strings.TrimSpace(rec[0])), strings.TrimSpace(rec[1]), strings.TrimSpace(rec[2])
		switch {
		case sym == "":
			errs = append(errs, fmt.Errorf("line %d: empty symbol", line))
		case seen[sym]:
			errs = append(errs, fmt.Errorf("line %d: duplicate symbol %s", line, sym))
		case id == "" && note == "":
			errs = append(errs, fmt.Errorf("line %d: excluded symbol %s needs a note saying which project the source's pair is", line, sym))
		case id == "":
			m.Excluded = append(m.Excluded, sym)
		case byID[id].AssetID == "":
			errs = append(errs, fmt.Errorf("line %d: asset %s is not in the registry", line, id))
		case strings.ToUpper(byID[id].Symbol) != sym:
			errs = append(errs, fmt.Errorf("line %d: asset %s has symbol %s, not %s", line, id, byID[id].Symbol, sym))
		default:
			m.Verified[sym] = id
		}
		seen[sym] = true
	}
	if len(errs) > 0 {
		return SymbolMap{}, errors.Join(errs...)
	}
	sort.Strings(m.Excluded)
	return m, nil
}

// AmbiguousSymbols returns upper-cased symbols shared by more than one asset.
// They are reported at startup because a symbol join cannot resolve them.
func AmbiguousSymbols(assets []Asset) []string {
	count := map[string]int{}
	for _, a := range assets {
		count[strings.ToUpper(a.Symbol)]++
	}
	var out []string
	for s, n := range count {
		if n > 1 {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}
