# Reclaiming disk from unused image derivatives

Proposal only — nothing here has been executed. Written 2026-07-28.

## The numbers

Measured from a real pipeline tile (media 38649, `disc-security-systems.jpg`):

| | |
|---|---|
| Original tile | 1200x775, 58 KB |
| Derivatives generated | 27 sizes, 302 KB |
| Amplification | **5.2x** |
| Files per upload | 28 |

Across ~3,469 listing tiles that is roughly **97,000 files and ~1.05 GB**, of
which about **970 MB is never served**. Only `listingpro-blog-grid` (372x240,
9 KB) appears in the markup; single listing pages use the unsuffixed original.

Going forward, `gt-tile-image-sizes.php` cuts new tiles to 4 files / ~79 KB.
This document covers the backlog already on disk.

## Why this needs care

The host exhausted its image-processing capacity on 2026-07-22 doing exactly
this kind of work, and left 46 listings without featured images. A
regeneration pass is the same workload, concentrated. It must be throttled and
resumable, and it should not run during a publishing sweep.

## Sequence

**1. Install and verify the trim plugin (prerequisite)**

Upload `gt-tile-image-sizes.php` via Plugins > Add New > Upload, activate, then
publish one listing and confirm the new attachment has 4 sizes rather than 28.
Until this is verified, regeneration would simply rebuild what it deleted.

**2. Mark existing tiles**

```
wp eval 'gt_tile_backfill_markers();'          # dry run — expect ~3,469
wp eval 'gt_tile_backfill_markers( false );'   # write the markers
```

Matches only attachments that are a published listing's featured image *and*
are exactly 1200x775, the tile generator's fixed output. Photographs uploaded
by business owners do not match and keep every size.

**3. Regenerate in small batches**

`wp media regenerate` deletes derivatives not in the current size set, so with
the markers in place it prunes rather than rebuilds.

```
wp media regenerate --only-missing=0 --yes \
  --post_type=attachment --posts_per_page=100 --offset=0
```

Run 100 at a time, checking host load between batches. ~35 batches total.
Start with a single batch of 10 and confirm: derivative count drops to 4, the
grid card still renders on `/location/glasgow/`, and the single listing page
hero is intact.

**4. Verify**

Re-run the size audit against a regenerated attachment and confirm
`listingpro-blog-grid` survives. Spot-check the homepage, a location page and
a category page for broken images.

## Requirements

WP-CLI over SSH. If the host has no shell access, the alternative is the
*Regenerate Thumbnails* plugin, which offers the same pruning through wp-admin
but with less control over batching — in which case run it well outside a
publishing sweep and watch disk during the pass.

## Rollback

Deactivating `gt-tile-image-sizes.php` and re-running `wp media regenerate`
restores all 28 sizes. Slow, but nothing is unrecoverable: the originals are
never touched at any point.

## Recommended timing

After the remaining 17 Phase-2 trades are published, so the pass runs once
against a stable library rather than twice. Until then the trim plugin alone
stops the problem growing.
