# Reference data

`trustwallet_assets.csv` is a snapshot of Trust Wallet's public assets registry
(https://github.com/trustwallet/assets), taken from
`https://assets-cdn.trustwallet.com/blockchains/ethereum/tokenlist.json` on 2026-09-29,
plus four native coins (BTC, ETH, BNB, SOL) added by hand. The `pairs` field was dropped;
the rest is verbatim.

It is loaded into the `assets` table once at service start and is the dimension the
price snapshots join to. It is not polled: the registry changes through pull requests
a few times a week, so a checked-in snapshot with a stated date is the honest shape.

To refresh it, re-run the extraction and update the date above. Symbols are not unique
across the registry (`YLD` appears twice), and a ticker symbol on an exchange is not an
asset identity at all, which is why the second file exists.

`symbol_map.csv` is the reviewed link from a Binance ticker symbol to the registry
asset it prices, one row per symbol Binance traded against USDT on 2026-09-30. A row
with an asset id was checked by hand: the exchange's pair and the registry's contract
are the same project. A row with an empty asset id was checked and excluded, with the
note saying which project the exchange's pair actually is (six at capture: GAS, ID,
JUP, LAYER, LUNA, MEME). A symbol absent from the file is never priced, so a new
listing needs a row here before it can publish a price. The service refuses to start
if a row names an asset the registry does not have, or whose symbol disagrees.
