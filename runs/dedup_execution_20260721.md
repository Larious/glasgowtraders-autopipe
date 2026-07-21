# Dedup + review resolution execution — 2026-07-21

## Run A (token 11:39): scripts/apply_resolutions.py --live
25 operations, 25 ok / 0 failed:
- 10 review-ACCEPT place_ids written (data/place_backfill_review_resolved.csv)
- 6 keeper fixes: place_id written to the keeper post, ledger repointed
  (Noble→28726, Cowal→27970, PR→29759, SW Auto→29717, Fusion→29677,
  Wilkie/Shower Surgeon→29463)
- 3 location terms added: Airdrie(173)→28726, Balloch(181)→27970,
  Airdrie(173)→28392

## Run B (token 11:48): bulk_delete_listings.py --csv data/dedup_trash_final.csv --live
11 trashed / 0 failed (trash-only, restorable):
29800, 29751, 29747, 26795 (Noble republishes); 31479 (Cowal); 29805 (PR);
29795 (SW Auto); 29790 (Fusion); 29470 (John Wilkie); 26770 (Lauder);
26927 (Create Bathroom = Barclay Erskine trading name).

Kept deliberately (insufficient evidence): FastFix 29505 / 247 Plumbing 29082
(phone-only link, addresses 10 km apart); CBY 29268 / CB 26785 (phones differ).

## Verification (cache-busted)
- Sampled trashed posts report status=trash; all sampled keepers publish.
- Sampled keeper standalone meta gt_google_place_id matches ledger.
- Ledger: 994 entries; no trashed post holds a ledger entry.

## Site state after
- Published listings: ~1,012 (1,023 + ~... net of 11 trashes; next audit will
  give the exact count).
- Ledger staged: data/place_ledger.jsonl (994).
