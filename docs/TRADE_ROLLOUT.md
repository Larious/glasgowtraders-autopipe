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
- [x] joiner (162) — 16/16 core towns
- [x] painter (160) — 16/16 core towns
- [x] plasterer (163) — 16/16 core towns
- [x] kitchen (293) — 16/16 core towns
- [x] bathroom (169) — 16/16 core towns
- [x] tiler (170) — 16/16 core towns
- [x] handyman (167) — 16/16 core towns
- [x] heating (291; rules → boiler 172 / central heating 171) — 16/16 core towns
- [x] locksmith (168) — 16/16 core towns
- [x] window (297) — 16/16 core towns
- [x] fencing (165) — 16/16 core towns


### Phase 2 trades (configured 2026-07-22)
- [x] driveway (164)   - [x] flooring (287)    - [x] removals (308)
- [x] auto (311)       - [x] scaffolder (300)  - [ ] solar (305)
- [ ] cctv (304)       - [ ] damp (284)        - [ ] chimney (282)
- [ ] loft (294)       - [ ] conservatory (283)- [ ] carpet (281)
- [ ] bricklayer (280) - [ ] stonemason (306)  - [ ] insulation (292)
- [ ] demolition (285) - [ ] garage_builder (288) - [ ] aircon (307)
- [ ] hvac (309)       - [ ] hometech (301)    - [ ] guttering (290)
- [ ] construction (298) - [ ] extension (286) - [ ] wallpaper (313)

Note: construction, extension, guttering, hvac and wallpaper overlap
already-populated categories (Builder, Roofer, Heating, Painter). Their
businesses are mostly in the ledger already, so expect low net-new and
location attaches instead. That is correct one-business-one-listing behaviour.

## Backlog — remaining empty categories
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
