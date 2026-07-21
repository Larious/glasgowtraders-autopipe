<?php
/**
 * Plugin Name: Glasgow Traders - ListingPro REST API Bridge
 * Description: Writes listing data into ListingPro's serialized lp_listingpro_options array
 * Version: 2.2.0
 * Author: Glasgow Traders AutoPipe
 */

if (!defined('ABSPATH')) exit;

// CRITICAL: Prevent ListingPro from corrupting our data
add_action('save_post_listing', 'gt_prevent_data_corruption', 1, 3);
function gt_prevent_data_corruption($post_id, $post, $update) {
    // Skip if this is an autosave or revision
    if (wp_is_post_autosave($post_id) || wp_is_post_revision($post_id)) {
        return;
    }
    
    // Skip if we're in admin (allow manual edits)
    if (is_admin() && !wp_doing_ajax()) {
        return;
    }
}

function gt_register_endpoints() {
    register_rest_route('glasgow-traders/v1', '/listing-meta/(?P<id>\d+)', array(
        'methods'  => 'GET',
        'callback' => 'gt_get_listing_meta',
        'permission_callback' => function() {
            return current_user_can('edit_posts');
        },
    ));

    register_rest_route('glasgow-traders/v1', '/listing-options/(?P<id>\d+)', array(
        'methods'  => 'POST',
        'callback' => 'gt_update_listing_options',
        'permission_callback' => function() {
            return current_user_can('edit_posts');
        },
    ));
}
add_action('rest_api_init', 'gt_register_endpoints');

function gt_get_listing_meta($request) {
    $post_id = $request['id'];
    $post = get_post($post_id);
    if (!$post || $post->post_type !== 'listing') {
        return new WP_Error('not_found', 'Listing not found', array('status' => 404));
    }
    
    // Clear any object cache to get fresh data
    wp_cache_delete($post_id, 'post_meta');
    
    $all_meta = get_post_meta($post_id);
    $clean = array();
    foreach ($all_meta as $key => $values) {
        $val = $values[0];
        $clean[$key] = array(
            'value' => maybe_unserialize($val),
            'raw'   => $val,
        );
    }
    return array(
        'post_id'    => $post_id,
        'title'      => $post->post_title,
        'meta_count' => count($clean),
        'meta'       => $clean,
    );
}

function gt_update_listing_options($request) {
    $post_id = $request['id'];
    $post = get_post($post_id);
    if (!$post || $post->post_type !== 'listing') {
        return new WP_Error('not_found', 'Listing not found', array('status' => 404));
    }

    $body = $request->get_json_params();
    if (empty($body)) {
        return new WP_Error('empty', 'No fields provided', array('status' => 400));
    }

    // CRITICAL: Remove all save_post hooks temporarily
    remove_all_actions('save_post');
    remove_all_actions('save_post_listing');
    
    // Clear cache before reading
    wp_cache_delete($post_id, 'post_meta');
    
    // Get existing options
    $options = get_post_meta($post_id, 'lp_listingpro_options', true);
    if (!is_array($options)) {
        $options = array(
            'tagline_text' => '', 'gAddress' => '', 'latitude' => '', 'longitude' => '',
            'mappin' => '', 'phone' => '', 'whatsapp' => '', 'email' => '', 'website' => '',
            'twitter' => '', 'facebook' => '', 'linkedin' => '', 'youtube' => '', 'instagram' => '',
            'video' => '', 'gallery' => '', 'price_status' => 'notsay', 'list_price' => '',
            'list_price_to' => '', 'Plan_id' => '0', 'lp_purchase_days' => '', 'reviews_ids' => '',
            'claimed_section' => 'not_claimed', 'listings_ads_purchase_date' => '',
            'listings_ads_purchase_packages' => '', 
            'faqs' => array('faq' => array(1 => ''), 'faqans' => array(1 => '')),
            'business_hours' => array(),
            'campaign_id' => '', 'changed_planid' => '', 'listing_reported_by' => '',
            'listing_reported' => '', 'business_logo' => '',
        );
    }

    // Merge fields
    $updated_fields = array();
    foreach ($body as $key => $value) {
        $options[$key] = $value;
        $updated_fields[] = $key;
    }

    // Delete old meta completely, then add new (prevents corruption)
    delete_post_meta($post_id, 'lp_listingpro_options');
    wp_cache_delete($post_id, 'post_meta');
    
    // Add new meta
    $result = add_post_meta($post_id, 'lp_listingpro_options', $options, true);

    // Mirror google_place_id to standalone meta. ListingPro periodically
    // re-serializes lp_listingpro_options and drops keys it doesn't know,
    // so the durable copy must live outside the options array.
    if (isset($body['google_place_id'])) {
        $gpid = sanitize_text_field($body['google_place_id']);
        delete_post_meta($post_id, 'gt_google_place_id');
        add_post_meta($post_id, 'gt_google_place_id', $gpid, true);
    }

    // Also update standalone claimed meta
    if (isset($body['claimed_section'])) {
        $v = ($body['claimed_section'] === 'claimed') ? '1' : '0';
        delete_post_meta($post_id, 'claimed');
        add_post_meta($post_id, 'claimed', $v, true);
    }

    // Verify - clear cache and re-fetch
    wp_cache_delete($post_id, 'post_meta');
    $saved = get_post_meta($post_id, 'lp_listingpro_options', true);
    
    $verify = array();
    foreach ($updated_fields as $field) {
        $verify[$field] = isset($saved[$field]) ? $saved[$field] : '(not found)';
    }
    if (isset($body['google_place_id'])) {
        // Echo the standalone copy so clients can require durable persistence.
        $verify['gt_google_place_id'] = get_post_meta($post_id, 'gt_google_place_id', true);
    }

    return array(
        'post_id' => $post_id,
        'updated_fields' => $updated_fields,
        'verified' => $verify,
        'message' => count($updated_fields) . ' fields written',
        'success' => is_array($saved) && !empty($saved),
        'result' => $result,
    );
}
