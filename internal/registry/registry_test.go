package registry

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

const header = "asset_id,coin_id,chain,contract_address,symbol,name,decimals,token_type\n"

func TestParseValid(t *testing.T) {
	// The trailing "logo" column is not required and must be ignored.
	in := "asset_id,coin_id,chain,contract_address,symbol,name,decimals,token_type,logo\n" +
		"c60,60,ethereum,,ETH,Ethereum,18,NATIVE,http://x\n" +
		"c60_t0xAbC,60,ethereum,0xAbC,sUSD,Synth USD,6,ERC20,\n"
	assets, err := Parse(strings.NewReader(in))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(assets) != 2 {
		t.Fatalf("want 2 assets, got %d", len(assets))
	}
	if assets[1].ContractAddress != "0xAbC" || assets[1].Decimals != 6 || assets[1].CoinID != 60 || assets[1].Symbol != "sUSD" {
		t.Errorf("row parsed wrong: %+v", assets[1])
	}
	if got := Symbols(assets); !reflect.DeepEqual(got, []string{"ETH", "SUSD"}) {
		t.Errorf("Symbols = %v", got)
	}
}

func TestParseRejects(t *testing.T) {
	cases := map[string]string{
		"missing column": "asset_id,coin_id\nc60,60\n",
		"duplicate id":   header + "c60,60,ethereum,,ETH,Ethereum,18,NATIVE\nc60,60,ethereum,,ETH,Ethereum,18,NATIVE\n",
		"bad decimals":   header + "c60,60,ethereum,,ETH,Ethereum,eighteen,NATIVE\n",
		"empty asset id": header + ",60,ethereum,,ETH,Ethereum,18,NATIVE\n",
		"empty symbol":   header + "c60,60,ethereum,,,Ethereum,18,NATIVE\n",
		"no rows":        header,
		"ragged row":     header + "c60,60\n",
		"empty file":     "",
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse(strings.NewReader(in)); err == nil {
				t.Fatalf("expected error")
			}
		})
	}
}

func TestAmbiguousSymbols(t *testing.T) {
	in := header +
		"c60_t1,60,ethereum,0x1,YLD,Yield A,18,ERC20\n" +
		"c60_t2,60,ethereum,0x2,yld,Yield B,18,ERC20\n" +
		"c60,60,ethereum,,ETH,Ethereum,18,NATIVE\n"
	assets, err := Parse(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if got := AmbiguousSymbols(assets); !reflect.DeepEqual(got, []string{"YLD"}) {
		t.Errorf("AmbiguousSymbols = %v", got)
	}
	if got := Symbols(assets); len(got) != 2 {
		t.Errorf("Symbols should be distinct: %v", got)
	}
}

// TestSymbolMap: verified rows bind a symbol to a registry asset whose own
// symbol agrees; excluded rows need a note; anything else is an error.
func TestSymbolMap(t *testing.T) {
	assets, err := Parse(strings.NewReader(header +
		"c0,0,bitcoin,,BTC,Bitcoin,8,NATIVE\n" +
		"c60_t0xa,60,ethereum,0xa,GAS,Gas DAO,18,ERC20\n" +
		"c60_t0xb,60,ethereum,0xb,LINK,Chainlink,18,ERC20\n"))
	if err != nil {
		t.Fatal(err)
	}
	m, err := ParseSymbolMap(strings.NewReader("symbol,asset_id,note\nbtc,c0,\nGAS,,Binance GAS is Neo GAS\n"), assets)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(m.Verified, map[string]string{"BTC": "c0"}) || !reflect.DeepEqual(m.Excluded, []string{"GAS"}) {
		t.Errorf("map = %+v", m)
	}
	for name, in := range map[string]string{
		"unknown asset":       "symbol,asset_id,note\nBTC,c999,\n",
		"symbol mismatch":     "symbol,asset_id,note\nLINK,c0,\n",
		"exclusion no note":   "symbol,asset_id,note\nGAS,,\n",
		"duplicate symbol":    "symbol,asset_id,note\nBTC,c0,\nBTC,c0,\n",
		"wrong header":        "ticker,asset\nBTC,c0\n",
		"reviewed but absent": "symbol,asset_id,note\n,c0,\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseSymbolMap(strings.NewReader(in), assets); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

// TestCheckedInSnapshotLoads guards the files the service ships with: the
// registry, and the symbol map that must agree with it.
func TestCheckedInSnapshotLoads(t *testing.T) {
	path := "../../reference/trustwallet_assets.csv"
	if _, err := os.Stat(path); err != nil {
		t.Skip("snapshot not present")
	}
	assets, err := Load(path)
	if err != nil {
		t.Fatalf("checked-in registry does not load: %v", err)
	}
	if len(assets) < 100 {
		t.Errorf("suspiciously small registry: %d rows", len(assets))
	}
	if amb := AmbiguousSymbols(assets); len(amb) > 5 {
		t.Errorf("too many ambiguous symbols, check the snapshot: %v", amb)
	}
	m, err := LoadSymbolMap("../../reference/symbol_map.csv", assets)
	if err != nil {
		t.Fatalf("checked-in symbol map does not agree with the registry: %v", err)
	}
	if len(m.Verified) < 50 || len(m.Excluded) < 1 {
		t.Errorf("symbol map: %d verified, %d excluded", len(m.Verified), len(m.Excluded))
	}
}
