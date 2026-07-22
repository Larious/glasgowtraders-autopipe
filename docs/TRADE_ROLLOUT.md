# Trade-category rollout plan

Goal: populate every trade category with real UK businesses across the
region. Decisions (2026-07-22): **core towns first**, **high-demand trades
first**.

## Method
`batch_snipe_location.py --trade <key> --location <town>` per trade × town.
- UK-guarded (`, Scotland, UK` query + `is_uk_address` filter).
- Ledger-deduped: a business already published (any trade) is skipped and the
  town attached to its existing post — so one business = one listing, one
  category. Overlapping trades therefore populate a category only with the
  businesses uniquely surfaced by that trade's search.
- Every listing ships SEO-complete (branded tile + category + Yoast).

## Phase 1 — core towns (16) per trade
CORE_TOWNS = Glasgow, Paisley, Airdrie, Coatbridge, Bellshill, Hamilton,
East Kilbride, Clydebank, Cumbernauld, Dumbarton, Ayr, Greenock, Falkirk,
Stirling, Bathgate, Carluke.

### High-demand trades (configured, Phase-1 target)
- [x] roofer (158) — 16/16 core towns
- [ ] joiner (162)
- [ ] painter (160)
- [ ] plasterer (163)
- [ ] kitchen (293)
- [ ] bathroom (169)
- [ ] tiler (170)
- [ ] handyman (167)
- [ ] heating (291; rules → boiler 172 / central heating 171)
- [ ] locksmith (168)
- [ ] window (297)
- [ ] fencing (165)

## Backlog — remaining 26 empty categories (config not yet written)
Air Conditioning, Auto Workshop, Carpet Fitters, CCTV, Chimney/Fireplace,
Conservatory, Construction Contractors, Damp Proofing, Demolition, Driveways,
Extension builders, Flooring, Garage/Shed Builders, Guttering, Home Technology,
HVAC, Insulation, Loft Conversion, Removals, Scaffolders, Solar Panel,
Stonemasons, Wallpapers, Window (done), plus Bricklayers.

## Phase 2 — location-page depth (optional, per trade)
Suburb sweeps for full 107-location coverage. Expensive tail; do selectively.

## Cost/scale note
Full 44×107 matrix ≈ 4,700 runs / ~$500-800 / ~150-200h — not brute-forced.
Core-towns approach ≈ 16 runs/trade; dedup fills suburb location-pages via
attaches on later runs.
