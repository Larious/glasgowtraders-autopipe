<?php
/**
 * Plugin Name: GT Tile Image Sizes
 * Description: Skips the intermediate image sizes that generated listing tiles never use. Uploads from wp-admin, business owners, or the ListingPro claim flow are untouched.
 * Version:     1.1.0
 * Author:      Glasgow Trader
 *
 * Background
 * ----------
 * ListingPro + WP register 28 image sizes, so every upload writes 28 files:
 * ~302 KB of derivatives for a 58 KB tile, a 5.2x amplification. On 2026-07-22
 * that exhausted the host's image processing mid-sweep (derivative counts
 * decayed 28 -> 13 -> 2, then uploads 500'd outright) and left 46 listings
 * published with no featured image.
 *
 * Measured against the live site on 2026-07-28 — homepage, single listing
 * pages, /location/ pages, /listing-category/ pages — exactly ONE registered
 * size is ever served for a listing's featured image: listingpro-blog-grid
 * (372x240), used by the grid cards. Single listing pages render the
 * unsuffixed original.
 *
 * For our own generated tiles we therefore keep that size plus WP core's
 * thumbnail and medium (wp-admin's media library and the block editor expect
 * them) and skip the other 24. Roughly 302 KB -> 21 KB per tile, a 93% cut.
 *
 * This deliberately does NOT trim sizes globally. The gallery sizes look
 * unused only because our listings have no galleries yet; business owners
 * claiming a listing will upload real photos and need them.
 */

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

const GT_TILE_META_KEY = '_gt_tile';

/**
 * The only sizes a generated tile needs.
 *
 * Verified empirically rather than assumed — see the plugin header. If the
 * theme's card markup changes, re-measure before editing this list.
 */
function gt_tile_kept_sizes() {
	return apply_filters( 'gt_tile_kept_sizes', array(
		'listingpro-blog-grid', // 372x240 — grid cards, the one size actually served
		'thumbnail',            // 150x150 — wp-admin media library
		'medium',               // 300x194 — block editor / RSS
	) );
}

/**
 * Is the current REST request uploading a pipeline-generated tile?
 *
 * The publisher opts in explicitly by appending ?gt_tile=1 to the media POST,
 * so anything arriving by another route — wp-admin, the claim flow, an app —
 * is never flagged and keeps the full set of sizes.
 *
 * This is a routing hint, not an authorisation check: WordPress has already
 * authenticated the upload before this runs, and the flag only ever removes
 * derivatives, it never grants access.
 */
function gt_is_tile_request() {
	return isset( $_GET['gt_tile'] ) && '1' === sanitize_text_field( wp_unslash( $_GET['gt_tile'] ) );
}

/**
 * Mark tile attachments permanently.
 *
 * The query parameter only exists during the upload request. Without a durable
 * marker, a later `wp media regenerate` would rebuild all 28 sizes and quietly
 * undo the trim, so record it on the attachment itself.
 */
function gt_tile_mark_attachment( $attachment_id ) {
	if ( gt_is_tile_request() ) {
		update_post_meta( $attachment_id, GT_TILE_META_KEY, 1 );
	}
}
add_action( 'add_attachment', 'gt_tile_mark_attachment' );

/**
 * Should this attachment get the trimmed size set?
 *
 * Checks the durable marker first so regeneration behaves the same as the
 * original upload, then falls back to the request flag for the upload itself
 * (in case add_attachment did not fire first).
 */
function gt_tile_is_tile_attachment( $attachment_id ) {
	if ( $attachment_id && get_post_meta( $attachment_id, GT_TILE_META_KEY, true ) ) {
		return true;
	}
	return gt_is_tile_request();
}

/**
 * Drop the sizes a tile will never render.
 */
function gt_tile_trim_sizes( $sizes, $image_meta = array(), $attachment_id = 0 ) {
	if ( ! gt_tile_is_tile_attachment( $attachment_id ) ) {
		return $sizes;
	}

	$trimmed = array_intersect_key( $sizes, array_flip( gt_tile_kept_sizes() ) );

	// Never return an empty set: if the theme renames its sizes, fall back to
	// the full list rather than silently shipping imageless listings again.
	return empty( $trimmed ) ? $sizes : $trimmed;
}
add_filter( 'intermediate_image_sizes_advanced', 'gt_tile_trim_sizes', 10, 3 );

/**
 * Exact pixel dimensions of a pipeline-generated tile (scripts/tile_gen.py).
 * Used to tell our tiles apart from photographs uploaded by business owners.
 */
const GT_TILE_WIDTH  = 1200;
const GT_TILE_HEIGHT = 775;

/**
 * WP-CLI helper: mark existing tile attachments so a regeneration pass prunes
 * them instead of rebuilding all 28 sizes.
 *
 *   wp eval 'gt_tile_backfill_markers();'         # dry run, counts only
 *   wp eval 'gt_tile_backfill_markers( false );'  # write the markers
 *
 * Only matches attachments that are the featured image of a published listing
 * AND are exactly 1200x775 — the tile generator's fixed output. A photo
 * uploaded by a business owner will not match, so claimed listings keep their
 * full set of sizes. Makes no file changes; only writes post meta.
 */
function gt_tile_backfill_markers( $dry_run = true ) {
	global $wpdb;

	$ids = $wpdb->get_col(
		"SELECT DISTINCT pm.meta_value
		 FROM {$wpdb->postmeta} pm
		 INNER JOIN {$wpdb->posts} p ON p.ID = pm.post_id
		 WHERE pm.meta_key = '_thumbnail_id'
		   AND p.post_type = 'listing'
		   AND p.post_status = 'publish'"
	);

	$marked = 0;
	$skipped = 0;
	foreach ( $ids as $id ) {
		if ( get_post_meta( $id, GT_TILE_META_KEY, true ) ) {
			continue;
		}

		$meta = wp_get_attachment_metadata( $id );
		$is_tile = ! empty( $meta['width'] ) && ! empty( $meta['height'] )
			&& GT_TILE_WIDTH === (int) $meta['width']
			&& GT_TILE_HEIGHT === (int) $meta['height'];

		if ( ! $is_tile ) {
			$skipped++;
			continue;
		}

		if ( ! $dry_run ) {
			update_post_meta( $id, GT_TILE_META_KEY, 1 );
		}
		$marked++;
	}

	printf(
		"%s: %d listing featured images — %d match the tile fingerprint (%dx%d), %d left alone.\n",
		$dry_run ? 'DRY RUN' : 'LIVE',
		count( $ids ),
		$marked,
		GT_TILE_WIDTH,
		GT_TILE_HEIGHT,
		$skipped
	);
	return $marked;
}
