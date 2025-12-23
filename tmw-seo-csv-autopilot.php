<?php
/*
Plugin Name: TMW SEO CSV Autopilot (No OpenAI)
Description: CSV-first SEO autopilot for adult websites: imports Titles/H2 CSVs, applies RankMath meta, injects H2 blocks, adds adult meta + video schema, runs in batches.
Version: 1.0.0
Author: TMW
*/


if (!defined('ABSPATH')) { exit; }

class TMW_SEO_CSV_Autopilot {

  const OPT_SETTINGS = 'tmwseo_csv_settings_v1';
  const OPT_LOGS     = 'tmwseo_csv_logs_v1';
  const CRON_HOOK    = 'tmwseo_csv_batch_cron_v1';

  public static function boot() {
    register_activation_hook(__FILE__, [__CLASS__, 'activate']);
    register_deactivation_hook(__FILE__, [__CLASS__, 'deactivate']);

    add_action('admin_menu', [__CLASS__, 'admin_menu']);
    add_action('admin_post_tmwseo_csv_save_settings', [__CLASS__, 'handle_save_settings']);
    add_action('admin_post_tmwseo_csv_import', [__CLASS__, 'handle_import']);
    add_action('admin_post_tmwseo_csv_start_batch', [__CLASS__, 'handle_start_batch']);
    add_action('admin_post_tmwseo_csv_run_now', [__CLASS__, 'handle_run_now']);
    add_action('admin_post_tmwseo_csv_reset_progress', [__CLASS__, 'handle_reset_progress']);
    add_action('admin_post_tmwseo_csv_test_lookup', [__CLASS__, 'handle_test_lookup']);
    add_action('admin_post_tmwseo_csv_backfill_model_ids', [__CLASS__, 'handle_backfill_model_ids']);
    add_action('admin_post_tmwseo_csv_backfill_video_page_ids', [__CLASS__, 'handle_backfill_video_page_ids']);
    add_action('admin_post_tmwseo_csv_backfill_video_ids', [__CLASS__, 'handle_backfill_video_ids']);

    add_action(self::CRON_HOOK, [__CLASS__, 'run_batch']);

    // Adult SEO + schema
    add_action('wp_head', [__CLASS__, 'output_adult_rating_meta'], 1);
    add_action('wp_head', [__CLASS__, 'output_video_schema'], 20);

    // Sitemap endpoint
    add_action('init', [__CLASS__, 'rewrite_init']);
    add_filter('query_vars', function($vars){ $vars[] = 'tmwseo_video_sitemap'; return $vars; });
    add_action('template_redirect', [__CLASS__, 'maybe_output_video_sitemap']);
  }

  /** -------------------------
   * Activation / Tables
   * ------------------------ */
  public static function activate() {
    self::create_tables();
    self::rewrite_init();
    flush_rewrite_rules();
    self::log('Activated plugin and ensured DB tables + rewrite rules.');
  }

  public static function deactivate() {
    wp_clear_scheduled_hook(self::CRON_HOOK);
    flush_rewrite_rules();
    self::log('Deactivated plugin and cleared cron hook.');
  }

  private static function create_tables() {
    global $wpdb;
    require_once ABSPATH . 'wp-admin/includes/upgrade.php';

    $charset = $wpdb->get_charset_collate();

    $t_titles  = self::table('tmwseo_titles');
    $t_vh2     = self::table('tmwseo_video_h2');
    $t_mh2     = self::table('tmwseo_model_h2');
    $t_mh2nt   = self::table('tmwseo_model_h2_nt');

    $sql1 = "CREATE TABLE $t_titles (
      video_id VARCHAR(32) NOT NULL,
      keyword_slot VARCHAR(16) NOT NULL,
      focus_keyword VARCHAR(128) NOT NULL,
      seo_title VARCHAR(255) NOT NULL,
      tone VARCHAR(64) NULL,
      category VARCHAR(64) NULL,
      source_longtail VARCHAR(255) NULL,
      PRIMARY KEY (video_id, keyword_slot),
      KEY video_id (video_id)
    ) $charset;";

    $sql2 = "CREATE TABLE $t_vh2 (
      page_id VARCHAR(32) NOT NULL,
      h2_1 VARCHAR(255) NOT NULL,
      h2_2 VARCHAR(255) NOT NULL,
      h2_3 VARCHAR(255) NOT NULL,
      h2_4 VARCHAR(255) NOT NULL,
      PRIMARY KEY (page_id)
    ) $charset;";

    $sql3 = "CREATE TABLE $t_mh2 (
      page_id VARCHAR(32) NOT NULL,
      trait VARCHAR(128) NULL,
      h2_1 VARCHAR(255) NOT NULL,
      h2_2 VARCHAR(255) NOT NULL,
      h2_3 VARCHAR(255) NOT NULL,
      h2_4 VARCHAR(255) NOT NULL,
      PRIMARY KEY (page_id)
    ) $charset;";

    $sql4 = "CREATE TABLE $t_mh2nt (
      page_id VARCHAR(32) NOT NULL,
      h2_1 VARCHAR(255) NOT NULL,
      h2_2 VARCHAR(255) NOT NULL,
      h2_3 VARCHAR(255) NOT NULL,
      h2_4 VARCHAR(255) NOT NULL,
      PRIMARY KEY (page_id)
    ) $charset;";

    dbDelta($sql1);
    dbDelta($sql2);
    dbDelta($sql3);
    dbDelta($sql4);
  }

  private static function table($suffix) {
    global $wpdb;
    return $wpdb->prefix . $suffix;
  }

  /** -------------------------
   * Settings + Admin UI
   * ------------------------ */
  public static function admin_menu() {
    add_menu_page(
      'TMW SEO CSV Autopilot',
      'TMW SEO CSV',
      'manage_options',
      'tmwseo-csv',
      [__CLASS__, 'render_admin_page'],
      'dashicons-chart-line'
    );
  }

  private static function defaults() {
    return [
      // Mapping
      'mapping_mode' => 'meta', // meta|slug
      'video_id_meta_key' => '_tmw_video_id',
      'page_id_meta_key'  => '_tmw_page_id',

      // Post types
      'titles_post_types' => 'video',
      'video_h2_post_types' => 'video_page',
      'model_post_types' => 'model',

      // Model H2 source: trait|no_trait
      'model_h2_source' => 'no_trait',

      // RankMath + writing
      'update_wp_title' => '0',
      'write_rankmath'  => '1',

      // Adult SEO
      'adult_meta_post_types' => 'video,model,video_page',
      'output_video_schema_post_types' => 'video,video_page',

      // Safety
      'hard_block_regex' => '', // user pastes
      'soft_replace_json' => json_encode([
        'cam girl' => 'live creator',
        'cam' => 'stream',
        'webcam' => 'live stream',
        'adult' => 'live',
        'private' => '1-on-1',
        'after dark' => 'late night',
      ], JSON_PRETTY_PRINT),

      // Files (uploaded paths)
      'titles_csv' => '',
      'video_h2_csv' => '',
      'model_h2_csv' => '',
      'model_h2_nt_csv' => '',

      // Batch
      'batch_size' => 150,
      'progress' => [
        'titles_last_id' => 0,
        'video_h2_last_id' => 0,
        'model_last_id' => 0,
        'running' => 0,
      ],
    ];
  }

  private static function get_settings() {
    $s = get_option(self::OPT_SETTINGS, []);
    $d = self::defaults();
    // Deep merge for progress
    if (!isset($s['progress']) || !is_array($s['progress'])) { $s['progress'] = []; }
    $s['progress'] = array_merge($d['progress'], $s['progress']);
    return array_merge($d, $s);
  }

  private static function save_settings($settings) {
    update_option(self::OPT_SETTINGS, $settings, false);
  }

  public static function render_admin_page() {
    if (!current_user_can('manage_options')) return;

    $s = self::get_settings();
    $logs = get_option(self::OPT_LOGS, []);
    if (!is_array($logs)) $logs = [];

    echo '<div class="wrap">';
    echo '<h1>TMW SEO CSV Autopilot (No OpenAI)</h1>';

    echo '<p><strong>Workflow:</strong> 1) Paste hard-block regex 2) Upload CSVs 3) Import to DB 4) Start Batch Apply</p>';

    if (isset($_GET['tmwseo_backfill_notice'])) {
      $notice = sanitize_text_field(wp_unslash($_GET['tmwseo_backfill_notice']));
      if ($notice !== '') {
        echo '<div class="notice notice-success"><p>' . esc_html($notice) . '</p></div>';
      }
    }

    // SETTINGS FORM
    echo '<h2>Settings</h2>';
    echo '<form method="post" action="'.esc_url(admin_url('admin-post.php')).'">';
    echo '<input type="hidden" name="action" value="tmwseo_csv_save_settings" />';
    wp_nonce_field('tmwseo_csv_save_settings');

    echo '<table class="form-table"><tbody>';

    echo '<tr><th>Mapping mode</th><td>';
    echo '<label><input type="radio" name="mapping_mode" value="meta" '.checked($s['mapping_mode'], 'meta', false).' /> Meta key</label> ';
    echo '<label><input type="radio" name="mapping_mode" value="slug" '.checked($s['mapping_mode'], 'slug', false).' /> Slug</label>';
    echo '</td></tr>';

    echo '<tr><th>Video ID meta key</th><td><input type="text" name="video_id_meta_key" value="'.esc_attr($s['video_id_meta_key']).'" class="regular-text" /> <p class="description">Used to find CSV video_id like <code>video_0001</code> (if mapping mode = meta).</p></td></tr>';
    echo '<tr><th>Page ID meta key</th><td><input type="text" name="page_id_meta_key" value="'.esc_attr($s['page_id_meta_key']).'" class="regular-text" /> <p class="description">Used to find CSV page_id like <code>page_00001</code> (if mapping mode = meta).</p></td></tr>';

    echo '<tr><th>Post types for Titles CSV</th><td><input type="text" name="titles_post_types" value="'.esc_attr($s['titles_post_types']).'" class="regular-text" /> <p class="description">Comma-separated, e.g. <code>video</code></p></td></tr>';
    echo '<tr><th>Post types for Video H2 CSV</th><td><input type="text" name="video_h2_post_types" value="'.esc_attr($s['video_h2_post_types']).'" class="regular-text" /> <p class="description">Comma-separated, e.g. <code>video_page</code></p></td></tr>';
    echo '<tr><th>Post types for Model H2 CSV</th><td><input type="text" name="model_post_types" value="'.esc_attr($s['model_post_types']).'" class="regular-text" /> <p class="description">Comma-separated, e.g. <code>model</code></p></td></tr>';

    echo '<tr><th>Model H2 source</th><td>';
    echo '<select name="model_h2_source">';
    echo '<option value="trait" '.selected($s['model_h2_source'], 'trait', false).'>Use model_h2 (+trait) CSV table</option>';
    echo '<option value="no_trait" '.selected($s['model_h2_source'], 'no_trait', false).'>Use model_h2 (no-trait) CSV table</option>';
    echo '</select></td></tr>';

    echo '<tr><th>Write RankMath meta</th><td><label><input type="checkbox" name="write_rankmath" value="1" '.checked($s['write_rankmath'], '1', false).' /> Enabled</label></td></tr>';
    echo '<tr><th>Update WP post title</th><td><label><input type="checkbox" name="update_wp_title" value="1" '.checked($s['update_wp_title'], '1', false).' /> Also set WordPress post title = SEO title</label></td></tr>';

    echo '<tr><th>Adult meta post types</th><td><input type="text" name="adult_meta_post_types" value="'.esc_attr($s['adult_meta_post_types']).'" class="regular-text" /> <p class="description">Comma-separated. Adds: <code>&lt;meta name="rating" content="adult"&gt;</code></p></td></tr>';
    echo '<tr><th>Video schema post types</th><td><input type="text" name="output_video_schema_post_types" value="'.esc_attr($s['output_video_schema_post_types']).'" class="regular-text" /></td></tr>';

    echo '<tr><th>Hard-block regex (SFW)</th><td>';
    echo '<textarea name="hard_block_regex" rows="5" class="large-text code" placeholder="Paste your strict regex here (case-insensitive + word boundaries).">'.esc_textarea($s['hard_block_regex']).'</textarea>';
    echo '<p class="description"><strong>Important:</strong> Plugin will NOT write any title/keyword/H2 that matches this regex.</p>';
    echo '</td></tr>';

    echo '<tr><th>Soft replace map (JSON)</th><td>';
    echo '<textarea name="soft_replace_json" rows="8" class="large-text code">'.esc_textarea($s['soft_replace_json']).'</textarea>';
    echo '<p class="description">Keys are replaced (case-insensitive) before safety check. Keep it SFW.</p>';
    echo '</td></tr>';

    echo '<tr><th>Batch size</th><td><input type="number" name="batch_size" value="'.esc_attr(intval($s['batch_size'])).'" min="10" max="1000" /></td></tr>';

    echo '</tbody></table>';

    submit_button('Save Settings');
    echo '</form>';

    // IMPORT FORM
    echo '<hr/><h2>Upload CSVs & Import to DB</h2>';
    echo '<form method="post" action="'.esc_url(admin_url('admin-post.php')).'" enctype="multipart/form-data">';
    echo '<input type="hidden" name="action" value="tmwseo_csv_import" />';
    wp_nonce_field('tmwseo_csv_import');

    echo '<table class="form-table"><tbody>';
    echo '<tr><th>Titles CSV</th><td><input type="file" name="titles_csv" accept=".csv" /> <p class="description">Expected columns: video_id, keyword_slot, focus_keyword, seo_title, tone, category, source_longtail</p><p>Current: <code>'.esc_html($s['titles_csv']).'</code></p></td></tr>';
    echo '<tr><th>Video H2 CSV</th><td><input type="file" name="video_h2_csv" accept=".csv" /> <p class="description">Expected columns: page_id, h2_1, h2_2, h2_3, h2_4</p><p>Current: <code>'.esc_html($s['video_h2_csv']).'</code></p></td></tr>';
    echo '<tr><th>Model H2 CSV (+trait)</th><td><input type="file" name="model_h2_csv" accept=".csv" /> <p class="description">Expected columns: page_id, h2_1..h2_4, trait</p><p>Current: <code>'.esc_html($s['model_h2_csv']).'</code></p></td></tr>';
    echo '<tr><th>Model H2 CSV (no trait)</th><td><input type="file" name="model_h2_nt_csv" accept=".csv" /> <p class="description">Expected columns: page_id, h2_1..h2_4</p><p>Current: <code>'.esc_html($s['model_h2_nt_csv']).'</code></p></td></tr>';
    echo '</tbody></table>';

    submit_button('Upload & Import CSV → DB');
    echo '</form>';

    // BATCH CONTROLS
    echo '<hr/><h2>Batch Apply</h2>';
    $running = intval($s['progress']['running']) === 1;

    echo '<p>Status: '.($running ? '<strong style="color:green">RUNNING</strong>' : '<strong style="color:#666">STOPPED</strong>').'</p>';
    echo '<p>Progress (last IDs): ';
    echo 'Titles='.$s['progress']['titles_last_id'].' | Video H2='.$s['progress']['video_h2_last_id'].' | Model='.$s['progress']['model_last_id'];
    echo '</p>';

    echo '<form method="post" action="'.esc_url(admin_url('admin-post.php')).'" style="display:inline-block;margin-right:10px;">';
    echo '<input type="hidden" name="action" value="tmwseo_csv_start_batch" />';
    wp_nonce_field('tmwseo_csv_start_batch');
    submit_button($running ? 'Kick Batch (still running)' : 'Start Batch Apply', 'primary', 'submit', false);
    echo '</form>';

    echo '<form method="post" action="'.esc_url(admin_url('admin-post.php')).'" style="display:inline-block;margin-right:10px;">';
    echo '<input type="hidden" name="action" value="tmwseo_csv_run_now" />';
    wp_nonce_field('tmwseo_csv_run_now');
    submit_button('Run batch now (single tick)', 'secondary', 'submit', false);
    echo '</form>';

    echo '<form method="post" action="'.esc_url(admin_url('admin-post.php')).'" style="display:inline-block;">';
    echo '<input type="hidden" name="action" value="tmwseo_csv_reset_progress" />';
    wp_nonce_field('tmwseo_csv_reset_progress');
    submit_button('Reset Progress (start over)', 'secondary', 'submit', false);
    echo '</form>';

    echo '<p class="description">Batch runs via WP-Cron every ~2 minutes while running.</p>';

    // BACKFILL TOOLS
    echo '<hr/><h2>Backfill ID Tools</h2>';
    echo '<p>Use these tools to backfill missing mapping meta values without overwriting existing ones.</p>';

    echo '<form method="post" action="' . esc_url(admin_url('admin-post.php')) . '" style="display:inline-block;margin-right:10px;">';
    echo '<input type="hidden" name="action" value="tmwseo_csv_backfill_model_ids" />';
    wp_nonce_field('tmwseo_csv_backfill_model_ids');
    submit_button('Backfill Model page IDs', 'secondary', 'submit', false);
    echo '</form>';

    echo '<form method="post" action="' . esc_url(admin_url('admin-post.php')) . '" style="display:inline-block;margin-right:10px;">';
    echo '<input type="hidden" name="action" value="tmwseo_csv_backfill_video_page_ids" />';
    wp_nonce_field('tmwseo_csv_backfill_video_page_ids');
    submit_button('Backfill Video-page IDs', 'secondary', 'submit', false);
    echo '</form>';

    echo '<form method="post" action="' . esc_url(admin_url('admin-post.php')) . '" style="display:inline-block;">';
    echo '<input type="hidden" name="action" value="tmwseo_csv_backfill_video_ids" />';
    wp_nonce_field('tmwseo_csv_backfill_video_ids');
    submit_button('Backfill Video IDs (Titles)', 'secondary', 'submit', false);
    echo '</form>';

    // DIAGNOSTICS
    $model_h2_source = ($s['model_h2_source'] === 'trait') ? 'model_trait' : 'model_nt';
    $model_h2_table = self::get_model_h2_table_name($model_h2_source);
    $sample_page_ids = self::get_model_h2_samples($model_h2_source);
    $sample_model_posts = self::get_model_posts_sample($s['model_post_types'], $model_h2_source);
    $lookup_result = self::get_lookup_result($model_h2_source);

    echo '<hr/><h2>Diagnostics</h2>';

    echo '<h3>Mapping configuration</h3>';
    echo '<ul>';
    echo '<li>Mapping mode: <strong>' . esc_html($s['mapping_mode']) . '</strong></li>';
    echo '<li>Video ID meta key: <code>' . esc_html($s['video_id_meta_key']) . '</code></li>';
    echo '<li>Page ID meta key: <code>' . esc_html($s['page_id_meta_key']) . '</code></li>';
    echo '<li>Model H2 source: <strong>' . esc_html($model_h2_source) . '</strong> (table: <code>' . esc_html($model_h2_table ?: 'N/A') . '</code>)</li>';
    echo '</ul>';

    echo '<h3>Sample model H2 page_ids (from current source)</h3>';
    if (!empty($sample_page_ids)) {
      echo '<p><code>' . esc_html(implode(', ', $sample_page_ids)) . '</code></p>';
    } else {
      echo '<p><em>No page_ids found in the selected model H2 table.</em></p>';
    }

    echo '<h3>Sample model posts</h3>';
    if (!empty($sample_model_posts)) {
      echo '<table class="widefat striped" style="max-width:920px;">';
      echo '<thead><tr><th>post_id</th><th>post_name (slug)</th><th>extracted page_id</th><th>CSV row?</th></tr></thead><tbody>';
      foreach ($sample_model_posts as $row) {
        $has_csv = $row['has_csv'] ? 'Yes' : 'No';
        echo '<tr>';
        echo '<td>' . esc_html($row['post_id']) . '</td>';
        echo '<td>' . esc_html($row['post_name']) . '</td>';
        echo '<td>' . esc_html($row['page_id']) . '</td>';
        echo '<td>' . esc_html($has_csv) . '</td>';
        echo '</tr>';
      }
      echo '</tbody></table>';
    } else {
      echo '<p><em>No sample posts found for configured model post types.</em></p>';
    }

    echo '<h3>Test lookup</h3>';
    echo '<form method="post" action="' . esc_url(admin_url('admin-post.php')) . '" style="max-width:520px;">';
    echo '<input type="hidden" name="action" value="tmwseo_csv_test_lookup" />';
    wp_nonce_field('tmwseo_csv_test_lookup');
    echo '<p><label for="tmwseo_lookup_post_id">Post ID:</label> <input type="number" name="lookup_post_id" id="tmwseo_lookup_post_id" min="1" class="small-text" /></p>';
    submit_button('Test lookup', 'secondary', 'submit', false);
    echo '</form>';

    if ($lookup_result !== null) {
      if (isset($lookup_result['error'])) {
        echo '<p style="color:#a00;"><strong>Error:</strong> ' . esc_html($lookup_result['error']) . '</p>';
      } else {
        echo '<p><strong>Lookup result:</strong> Post ' . esc_html($lookup_result['post_id']) . ' → page_id <code>' . esc_html($lookup_result['page_id']) . '</code> | CSV row: <strong>' . esc_html($lookup_result['has_csv'] ? 'Yes' : 'No') . '</strong></p>';
      }
    }

    // LOGS
    echo '<hr/><h2>Logs (latest first)</h2>';
    echo '<div style="background:#111;color:#0f0;padding:12px;border-radius:8px;max-height:340px;overflow:auto;font-family:monospace;font-size:12px;">';
    foreach (array_slice(array_reverse($logs), 0, 250) as $line) {
      echo esc_html($line) . "\n";
    }
    echo '</div>';

    echo '<hr/><h2>Video Sitemap</h2>';
    echo '<p>Video sitemap URL: <code>'.esc_html(home_url('/tmwseo-video-sitemap.xml')).'</code></p>';

    echo '</div>';
  }

  public static function handle_test_lookup() {
    if (!current_user_can('manage_options')) wp_die('No permission');
    check_admin_referer('tmwseo_csv_test_lookup');

    $post_id = isset($_POST['lookup_post_id']) ? absint($_POST['lookup_post_id']) : 0;

    $url = add_query_arg([ 'page' => 'tmwseo-csv' ], admin_url('admin.php'));
    if ($post_id > 0) {
      $url = add_query_arg(['lookup_post_id' => $post_id, 'tmwseo_csv_lookup_nonce' => wp_create_nonce('tmwseo_csv_diag_lookup')], $url);
    }

    wp_redirect($url);
    exit;
  }

  public static function handle_backfill_model_ids() {
    if (!current_user_can('manage_options')) wp_die('No permission');
    check_admin_referer('tmwseo_csv_backfill_model_ids');

    $s = self::get_settings();
    $meta_key = $s['page_id_meta_key'];
    $post_types = self::csv_list($s['model_post_types']);
    $source = ($s['model_h2_source'] === 'trait') ? 'model_trait' : 'model_nt';
    $table = self::get_model_h2_table_name($source);

    if (!$meta_key || empty($post_types) || !$table) {
      return self::redirect_backfill('Backfill Model page IDs skipped: missing meta key, post types, or source table.');
    }

    $page_ids = self::fetch_ordered_page_ids($table);
    $posts = self::fetch_all_post_ids($post_types);
    $result = self::assign_ids_to_posts($page_ids, $posts, $meta_key);

    $msg = sprintf(
      'Backfill Model page IDs: assigned %d (source rows=%d, posts=%d).',
      $result['assigned'],
      count($page_ids),
      count($posts)
    );
    self::log('[TMW-BATCH] ' . $msg);
    return self::redirect_backfill($msg);
  }

  public static function handle_backfill_video_page_ids() {
    if (!current_user_can('manage_options')) wp_die('No permission');
    check_admin_referer('tmwseo_csv_backfill_video_page_ids');

    $s = self::get_settings();
    $meta_key = $s['page_id_meta_key'];
    $post_types = self::csv_list($s['video_h2_post_types']);
    $table = self::table('tmwseo_video_h2');

    if (!$meta_key || empty($post_types)) {
      return self::redirect_backfill('Backfill Video-page IDs skipped: missing meta key or post types.');
    }

    $page_ids = self::fetch_ordered_page_ids($table);
    $posts = self::fetch_all_post_ids($post_types);
    $result = self::assign_ids_to_posts($page_ids, $posts, $meta_key);

    $msg = sprintf(
      'Backfill Video-page IDs: assigned %d (source rows=%d, posts=%d).',
      $result['assigned'],
      count($page_ids),
      count($posts)
    );
    self::log('[TMW-BATCH] ' . $msg);
    return self::redirect_backfill($msg);
  }

  public static function handle_backfill_video_ids() {
    if (!current_user_can('manage_options')) wp_die('No permission');
    check_admin_referer('tmwseo_csv_backfill_video_ids');

    $s = self::get_settings();
    $meta_key = $s['video_id_meta_key'];
    $post_types = self::csv_list($s['titles_post_types']);
    $video_ids = self::fetch_distinct_video_ids();
    $posts = self::fetch_all_post_ids($post_types);

    if (!$meta_key || empty($post_types)) {
      return self::redirect_backfill('Backfill Video IDs skipped: missing meta key or post types.');
    }

    $result = self::assign_ids_to_posts($video_ids, $posts, $meta_key);

    $msg = sprintf(
      'Backfill Video IDs (Titles): assigned %d (source rows=%d, posts=%d).',
      $result['assigned'],
      count($video_ids),
      count($posts)
    );
    self::log('[TMW-BATCH] ' . $msg);
    return self::redirect_backfill($msg);
  }

  private static function redirect_backfill($message) {
    $url = add_query_arg(
      [
        'page' => 'tmwseo-csv',
        'tmwseo_backfill_notice' => (string)$message,
      ],
      admin_url('admin.php')
    );
    wp_redirect($url);
    exit;
  }

  public static function handle_save_settings() {
    if (!current_user_can('manage_options')) wp_die('No permission');
    check_admin_referer('tmwseo_csv_save_settings');

    $s = self::get_settings();

    $s['mapping_mode'] = ($_POST['mapping_mode'] ?? 'meta') === 'slug' ? 'slug' : 'meta';
    $s['video_id_meta_key'] = sanitize_text_field($_POST['video_id_meta_key'] ?? $s['video_id_meta_key']);
    $s['page_id_meta_key'] = sanitize_text_field($_POST['page_id_meta_key'] ?? $s['page_id_meta_key']);

    $s['titles_post_types'] = sanitize_text_field($_POST['titles_post_types'] ?? $s['titles_post_types']);
    $s['video_h2_post_types'] = sanitize_text_field($_POST['video_h2_post_types'] ?? $s['video_h2_post_types']);
    $s['model_post_types'] = sanitize_text_field($_POST['model_post_types'] ?? $s['model_post_types']);
    $s['model_h2_source'] = ($_POST['model_h2_source'] ?? 'no_trait') === 'trait' ? 'trait' : 'no_trait';

    $s['write_rankmath'] = isset($_POST['write_rankmath']) ? '1' : '0';
    $s['update_wp_title'] = isset($_POST['update_wp_title']) ? '1' : '0';

    $s['adult_meta_post_types'] = sanitize_text_field($_POST['adult_meta_post_types'] ?? $s['adult_meta_post_types']);
    $s['output_video_schema_post_types'] = sanitize_text_field($_POST['output_video_schema_post_types'] ?? $s['output_video_schema_post_types']);

    $s['hard_block_regex'] = trim((string)($_POST['hard_block_regex'] ?? $s['hard_block_regex']));
    $s['soft_replace_json'] = (string)($_POST['soft_replace_json'] ?? $s['soft_replace_json']);

    $s['batch_size'] = max(10, min(1000, intval($_POST['batch_size'] ?? $s['batch_size'])));

    self::save_settings($s);
    self::log('Saved settings.');

    wp_redirect(admin_url('admin.php?page=tmwseo-csv'));
    exit;
  }

  /** -------------------------
   * Import CSVs
   * ------------------------ */
  public static function handle_import() {
    if (!current_user_can('manage_options')) wp_die('No permission');
    check_admin_referer('tmwseo_csv_import');

    $s = self::get_settings();

    $uploads = wp_upload_dir();
    $dest_dir = trailingslashit($uploads['basedir']) . 'tmwseo-csv/';
    if (!file_exists($dest_dir)) {
      wp_mkdir_p($dest_dir);
    }

    $files = [
      'titles_csv' => 'titles_csv',
      'video_h2_csv' => 'video_h2_csv',
      'model_h2_csv' => 'model_h2_csv',
      'model_h2_nt_csv' => 'model_h2_nt_csv',
    ];

    foreach ($files as $field => $setting_key) {
      if (!empty($_FILES[$field]['name'])) {
        $tmp = $_FILES[$field]['tmp_name'];
        $name = sanitize_file_name($_FILES[$field]['name']);
        $dest = $dest_dir . time() . '-' . $name;

        if (!is_uploaded_file($tmp)) {
          self::log("Upload failed (not uploaded file): $field");
          continue;
        }

        if (!move_uploaded_file($tmp, $dest)) {
          self::log("Upload failed (move): $field");
          continue;
        }

        $s[$setting_key] = $dest;
        self::log("Uploaded $field to $dest");
      }
    }

    self::save_settings($s);

    // Import each present file
    $ok = true;
    if (!empty($s['titles_csv']))  { $ok = self::import_titles($s['titles_csv']) && $ok; }
    if (!empty($s['video_h2_csv'])){ $ok = self::import_h2($s['video_h2_csv'], 'video') && $ok; }
    if (!empty($s['model_h2_csv'])){ $ok = self::import_h2($s['model_h2_csv'], 'model_trait') && $ok; }
    if (!empty($s['model_h2_nt_csv'])){ $ok = self::import_h2($s['model_h2_nt_csv'], 'model_nt') && $ok; }

    self::log($ok ? 'Import finished OK.' : 'Import finished with errors. See logs.');

    wp_redirect(admin_url('admin.php?page=tmwseo-csv'));
    exit;
  }

  private static function import_titles($path) {
    global $wpdb;
    $table = self::table('tmwseo_titles');

    $expected = ['video_id','keyword_slot','focus_keyword','seo_title','tone','category','source_longtail'];

    $fh = @fopen($path, 'r');
    if (!$fh) { self::log("Cannot open titles CSV: $path"); return false; }

    $header = fgetcsv($fh);
    if (!$header) { self::log("Empty titles CSV: $path"); fclose($fh); return false; }

    $header = array_map('trim', $header);
    $map = [];
    foreach ($expected as $col) {
      $idx = array_search($col, $header, true);
      if ($idx === false) {
        self::log("Titles CSV missing column: $col");
        fclose($fh);
        return false;
      }
      $map[$col] = $idx;
    }

    $count = 0;
    while (($row = fgetcsv($fh)) !== false) {
      $video_id = self::clean_id($row[$map['video_id']] ?? '');
      $slot     = sanitize_text_field($row[$map['keyword_slot']] ?? '');
      $focus    = self::clean_text($row[$map['focus_keyword']] ?? '', 128);
      $title    = self::clean_text($row[$map['seo_title']] ?? '', 255);
      $tone     = self::clean_text($row[$map['tone']] ?? '', 64);
      $cat      = self::clean_text($row[$map['category']] ?? '', 64);
      $longtail = self::clean_text($row[$map['source_longtail']] ?? '', 255);

      if (!$video_id || !$slot || !$focus || !$title) continue;

      $wpdb->replace($table, [
        'video_id' => $video_id,
        'keyword_slot' => $slot,
        'focus_keyword' => $focus,
        'seo_title' => $title,
        'tone' => $tone,
        'category' => $cat,
        'source_longtail' => $longtail,
      ], ['%s','%s','%s','%s','%s','%s','%s']);

      $count++;
      if ($count % 2000 === 0) self::log("Titles import progress: $count rows...");
    }
    fclose($fh);

    self::log("Titles imported: $count rows into $table");
    return true;
  }

  private static function import_h2($path, $type) {
    global $wpdb;

    if ($type === 'video') {
      $table = self::table('tmwseo_video_h2');
      $expected = ['page_id','h2_1','h2_2','h2_3','h2_4'];
    } elseif ($type === 'model_trait') {
      $table = self::table('tmwseo_model_h2');
      $expected = ['page_id','h2_1','h2_2','h2_3','h2_4','trait'];
    } else { // model_nt
      $table = self::table('tmwseo_model_h2_nt');
      $expected = ['page_id','h2_1','h2_2','h2_3','h2_4'];
    }

    $fh = @fopen($path, 'r');
    if (!$fh) { self::log("Cannot open H2 CSV: $path"); return false; }

    $header = fgetcsv($fh);
    if (!$header) { self::log("Empty H2 CSV: $path"); fclose($fh); return false; }
    $header = array_map('trim', $header);

    $map = [];
    foreach ($expected as $col) {
      $idx = array_search($col, $header, true);
      if ($idx === false) {
        self::log("H2 CSV missing column ($type): $col");
        fclose($fh);
        return false;
      }
      $map[$col] = $idx;
    }

    $count = 0;
    while (($row = fgetcsv($fh)) !== false) {
      $page_id = self::clean_id($row[$map['page_id']] ?? '');
      if (!$page_id) continue;

      $data = [
        'page_id' => $page_id,
        'h2_1' => self::clean_text($row[$map['h2_1']] ?? '', 255),
        'h2_2' => self::clean_text($row[$map['h2_2']] ?? '', 255),
        'h2_3' => self::clean_text($row[$map['h2_3']] ?? '', 255),
        'h2_4' => self::clean_text($row[$map['h2_4']] ?? '', 255),
      ];

      $formats = ['%s','%s','%s','%s','%s'];

      if ($type === 'model_trait') {
        $data['trait'] = self::clean_text($row[$map['trait']] ?? '', 128);
        $formats[] = '%s';
      }

      $wpdb->replace($table, $data, $formats);

      $count++;
      if ($count % 2000 === 0) self::log("$type H2 import progress: $count rows...");
    }
    fclose($fh);

    self::log("$type H2 imported: $count rows into $table");
    return true;
  }

  /** -------------------------
   * Batch apply
   * ------------------------ */
  public static function handle_start_batch() {
    if (!current_user_can('manage_options')) wp_die('No permission');
    check_admin_referer('tmwseo_csv_start_batch');

    $s = self::get_settings();
    $s['progress']['running'] = 1;
    self::save_settings($s);

    // Schedule soon and recurring
    if (!wp_next_scheduled(self::CRON_HOOK)) {
      wp_schedule_event(time() + 30, 'tmwseo_two_minutes', self::CRON_HOOK);
    }

    self::log('Batch started (cron scheduled).');
    wp_redirect(admin_url('admin.php?page=tmwseo-csv'));
    exit;
  }

  public static function handle_reset_progress() {
    if (!current_user_can('manage_options')) wp_die('No permission');
    check_admin_referer('tmwseo_csv_reset_progress');

    $s = self::get_settings();
    $s['progress']['titles_last_id'] = 0;
    $s['progress']['video_h2_last_id'] = 0;
    $s['progress']['model_last_id'] = 0;
    $s['progress']['running'] = 0;
    self::save_settings($s);

    wp_clear_scheduled_hook(self::CRON_HOOK);

    self::log('Progress reset and batch stopped.');
    wp_redirect(admin_url('admin.php?page=tmwseo-csv'));
    exit;
  }

  public static function handle_run_now() {
    if (!current_user_can('manage_options')) wp_die('No permission');
    check_admin_referer('tmwseo_csv_run_now');

    self::log('[TMW-BATCH] Manual batch tick requested.');
    self::run_batch(true);

    wp_redirect(admin_url('admin.php?page=tmwseo-csv'));
    exit;
  }

  public static function run_batch($forced = false) {
    $s = self::get_settings();
    $is_running = intval($s['progress']['running']) === 1;
    if (!$forced && !$is_running) return;

    $batch_size = max(10, min(1000, intval($s['batch_size'])));
    $did_work = false;

    // Apply Titles
    $did_work = self::apply_titles_batch($batch_size) || $did_work;

    // Apply Video H2
    $did_work = self::apply_h2_batch('video', $batch_size) || $did_work;

    // Apply Model H2
    $did_work = self::apply_h2_batch('model', $batch_size) || $did_work;

    if (!$did_work) {
      if ($is_running && !$forced) {
        // Stop if nothing left
        $s = self::get_settings();
        $s['progress']['running'] = 0;
        self::save_settings($s);
        wp_clear_scheduled_hook(self::CRON_HOOK);
        self::log('[TMW-BATCH] Batch finished: nothing left to process. Stopped.');
      } elseif ($forced) {
        self::log('[TMW-BATCH] Manual batch tick completed (no work this pass).');
      }
    }
  }

  private static function apply_titles_batch($batch_size) {
    $s = self::get_settings();
    $post_types = self::csv_list($s['titles_post_types']);
    if (empty($post_types)) return false;

    $last_id = intval($s['progress']['titles_last_id']);
    $ids = self::fetch_post_ids($post_types, $last_id, $batch_size);
    if (empty($ids)) return false;

    $max_id = $last_id;
    $count_ok = 0;

    foreach ($ids as $post_id) {
      $max_id = max($max_id, intval($post_id));
      $video_id = self::resolve_id_for_post($post_id, 'video');
      if (!$video_id) {
        self::log("[TMW-BATCH] Titles: missing video_id mapping for post $post_id");
        continue;
      }

      $rowset = self::get_titles_for_video($video_id);
      if (!$rowset || empty($rowset['keyword_1'])) {
        self::log("[TMW-BATCH] Titles: no CSV rows for video_id {$video_id} (post {$post_id})");
        continue;
      }

      $primary = $rowset['keyword_1'];
      $seo_title = self::safe_out($primary['seo_title'], $s);
      $focus_kw  = self::safe_out($primary['focus_keyword'], $s);

      if (!$seo_title || !$focus_kw) {
        self::log("SKIP Titles: blocked by safety for post $post_id ($video_id)");
        continue;
      }

      // Ensure title starts with focus keyword
      if (stripos($seo_title, $focus_kw) !== 0) {
        $seo_title = $focus_kw . ': ' . $seo_title;
        $seo_title = self::safe_out($seo_title, $s);
        if (!$seo_title) {
          self::log("SKIP Titles: start-fix caused block for post $post_id ($video_id)");
          continue;
        }
      }

      // Focus keywords list (up to 5)
      $all_kws = [];
      foreach (['keyword_1','keyword_2','keyword_3','keyword_4','keyword_5'] as $slot) {
        if (!empty($rowset[$slot]['focus_keyword'])) {
          $kw = self::safe_out($rowset[$slot]['focus_keyword'], $s);
          if ($kw) $all_kws[] = $kw;
        }
      }
      $all_kws = array_values(array_unique($all_kws));
      $focus_csv = implode(', ', array_slice($all_kws, 0, 5));

      // Description (SFW)
      $desc = self::build_description($seo_title, $focus_kw, $primary, $all_kws);
      $desc = self::safe_out($desc, $s);

      if ($s['write_rankmath'] === '1') {
        update_post_meta($post_id, 'rank_math_title', $seo_title);
        if ($desc) update_post_meta($post_id, 'rank_math_description', $desc);
        if ($focus_csv) update_post_meta($post_id, 'rank_math_focus_keyword', $focus_csv);
      }

      if ($s['update_wp_title'] === '1') {
        wp_update_post(['ID' => $post_id, 'post_title' => $seo_title]);
      }

      $count_ok++;
    }

    $s = self::get_settings();
    $s['progress']['titles_last_id'] = $max_id;
    self::save_settings($s);

    self::log("Batch Titles applied: $count_ok posts (last_id=$max_id)");
    return true;
  }

  private static function apply_h2_batch($mode, $batch_size) {
    $s = self::get_settings();

    if ($mode === 'video') {
      $post_types = self::csv_list($s['video_h2_post_types']);
      $progress_key = 'video_h2_last_id';
      $source = 'video';
    } else {
      $post_types = self::csv_list($s['model_post_types']);
      $progress_key = 'model_last_id';
      $source = ($s['model_h2_source'] === 'trait') ? 'model_trait' : 'model_nt';
    }

    if (empty($post_types)) return false;

    $last_id = intval($s['progress'][$progress_key]);
    $ids = self::fetch_post_ids($post_types, $last_id, $batch_size);
    if (empty($ids)) return false;

    $max_id = $last_id;
    $count_ok = 0;

    foreach ($ids as $post_id) {
      $max_id = max($max_id, intval($post_id));

      $page_id = self::resolve_id_for_post($post_id, 'page');
      if (!$page_id) {
        self::log("[TMW-BATCH] H2 {$mode}: missing page_id mapping for post $post_id");
        continue;
      }

      $h2 = self::get_h2_for_page($page_id, $source);
      if (!$h2) {
        self::log("[TMW-BATCH] H2 {$mode}: no CSV rows for page_id {$page_id} (post {$post_id})");
        continue;
      }

      // Safety on each H2
      $h2s = [];
      foreach (['h2_1','h2_2','h2_3','h2_4'] as $k) {
        $val = self::safe_out($h2[$k] ?? '', $s);
        if ($val) $h2s[] = $val;
      }
      if (count($h2s) < 4) {
        self::log("SKIP H2: blocked or missing for post $post_id ($page_id)");
        continue;
      }

      $content = get_post_field('post_content', $post_id);
      $new_block = self::build_h2_block_html($post_id, $h2s);

      $updated = self::replace_or_append_block($content, $new_block);

      if ($updated !== $content) {
        wp_update_post(['ID' => $post_id, 'post_content' => $updated]);
        $count_ok++;
      }
    }

    $s = self::get_settings();
    $s['progress'][$progress_key] = $max_id;
    self::save_settings($s);

    self::log("Batch H2 ($mode) applied: $count_ok posts (last_id=$max_id)");
    return true;
  }

  private static function fetch_post_ids($post_types, $last_id, $limit) {
    global $wpdb;
    if (empty($post_types)) return [];

    $post_types = array_filter(array_map('sanitize_key', $post_types));
    if (empty($post_types)) return [];

    $limit = max(1, intval($limit));
    $placeholders = implode(',', array_fill(0, count($post_types), '%s'));
    $sql = $wpdb->prepare(
      "SELECT ID FROM {$wpdb->posts} WHERE post_status='publish' AND post_type IN ($placeholders) AND ID > %d ORDER BY ID ASC LIMIT %d",
      array_merge($post_types, [$last_id, $limit])
    );

    $ids = $wpdb->get_col($sql);
    if (!is_array($ids)) return [];
    return array_map('intval', $ids);
  }

  private static function fetch_all_post_ids($post_types) {
    global $wpdb;
    if (empty($post_types)) return [];

    $post_types = array_filter(array_map('sanitize_key', $post_types));
    if (empty($post_types)) return [];

    $placeholders = implode(',', array_fill(0, count($post_types), '%s'));
    $sql = $wpdb->prepare(
      "SELECT ID FROM {$wpdb->posts} WHERE post_status='publish' AND post_type IN ($placeholders) ORDER BY ID ASC",
      $post_types
    );

    $ids = $wpdb->get_col($sql);
    if (!is_array($ids)) return [];
    return array_map('intval', $ids);
  }

  private static function fetch_ordered_page_ids($table) {
    global $wpdb;
    if (!$table) return [];
    $sql = "SELECT page_id FROM {$table} ORDER BY page_id ASC";
    $rows = $wpdb->get_col($sql);
    if (!is_array($rows)) return [];
    return array_values(array_map('strval', $rows));
  }

  private static function fetch_distinct_video_ids() {
    global $wpdb;
    $table = self::table('tmwseo_titles');
    $sql = "SELECT DISTINCT video_id FROM {$table} ORDER BY video_id ASC";
    $rows = $wpdb->get_col($sql);
    if (!is_array($rows)) return [];
    return array_values(array_map('strval', $rows));
  }

  /** -------------------------
   * Data lookups
   * ------------------------ */
  private static function get_titles_for_video($video_id) {
    global $wpdb;
    $table = self::table('tmwseo_titles');

    $rows = $wpdb->get_results($wpdb->prepare(
      "SELECT keyword_slot, focus_keyword, seo_title, tone, category, source_longtail
       FROM $table WHERE video_id = %s",
      $video_id
    ), ARRAY_A);

    if (!$rows) return null;

    $out = [];
    foreach ($rows as $r) {
      $slot = $r['keyword_slot'];
      $out[$slot] = $r;
    }
    return $out;
  }

  private static function get_h2_for_page($page_id, $source) {
    global $wpdb;

    if ($source === 'video') {
      $table = self::table('tmwseo_video_h2');
      $sql = "SELECT page_id, h2_1, h2_2, h2_3, h2_4 FROM $table WHERE page_id=%s";
    } elseif ($source === 'model_trait') {
      $table = self::table('tmwseo_model_h2');
      $sql = "SELECT page_id, trait, h2_1, h2_2, h2_3, h2_4 FROM $table WHERE page_id=%s";
    } else {
      $table = self::table('tmwseo_model_h2_nt');
      $sql = "SELECT page_id, h2_1, h2_2, h2_3, h2_4 FROM $table WHERE page_id=%s";
    }

    return $wpdb->get_row($wpdb->prepare($sql, $page_id), ARRAY_A);
  }

  /** -------------------------
   * Safety + text utils
   * ------------------------ */
  private static function clean_id($s) {
    $s = strtolower(trim((string)$s));
    $s = preg_replace('/[^a-z0-9_]/', '', $s);
    return substr($s, 0, 32);
  }

  private static function clean_text($s, $max) {
    $s = trim((string)$s);
    $s = preg_replace('/\s+/', ' ', $s);
    if (function_exists('mb_substr')) return mb_substr($s, 0, $max);
    return substr($s, 0, $max);
  }

  private static function safe_out($text, $settings) {
    $text = (string)$text;

    // Soft replaces first
    $map = json_decode($settings['soft_replace_json'] ?? '', true);
    if (is_array($map)) {
      foreach ($map as $from => $to) {
        if (!is_string($from) || $from === '') continue;
        $pattern = '/\b' . preg_quote($from, '/') . '\b/i';
        $text = preg_replace($pattern, (string)$to, $text);
      }
    }

    $text = preg_replace('/\s+/', ' ', trim($text));

    // Hard block
    $rx = trim((string)($settings['hard_block_regex'] ?? ''));
    if ($rx !== '') {
      $ok = @preg_match('/' . $rx . '/', 'test');
      // If user pasted delimiters already, try as-is
      if ($ok === false) {
        $ok2 = @preg_match($rx, 'test');
        if ($ok2 === false) {
          self::log("WARNING: hard_block_regex invalid. Not enforcing until fixed.");
          return $text;
        }
        if (@preg_match($rx, $text)) return '';
      } else {
        if (@preg_match('/' . $rx . '/', $text)) return '';
      }
    }

    return $text;
  }

  private static function csv_list($s) {
    $parts = array_filter(array_map('trim', explode(',', (string)$s)));
    return array_values(array_unique($parts));
  }

  /** -------------------------
   * ID resolution
   * ------------------------ */
  private static function resolve_id_for_post($post_id, $kind) {
    $s = self::get_settings();
    $mode = $s['mapping_mode'];

    $slug = get_post_field('post_name', $post_id);

    $meta_key = ($kind === 'video') ? $s['video_id_meta_key'] : $s['page_id_meta_key'];
    $meta_val = $meta_key ? get_post_meta($post_id, $meta_key, true) : '';

    $id = '';
    if ($mode === 'meta' && !empty($meta_val)) {
      $id = (string)$meta_val;
    } else {
      $id = (string)$slug;
    }

    $id = strtolower(trim($id));

    // Normalize numeric-only slugs/meta (optional)
    if (preg_match('/^\d+$/', $id)) {
      $n = intval($id);
      if ($kind === 'video') $id = sprintf('video_%04d', $n);
      else $id = sprintf('page_%05d', $n);
    }

    return self::clean_id($id);
  }

  private static function get_model_h2_table_name($source) {
    if ($source === 'model_trait') return self::table('tmwseo_model_h2');
    if ($source === 'model_nt') return self::table('tmwseo_model_h2_nt');
    return '';
  }

  private static function get_model_h2_samples($source) {
    global $wpdb;
    $table = self::get_model_h2_table_name($source);
    if (!$table) return [];
    $sql = "SELECT page_id FROM {$table} ORDER BY page_id ASC LIMIT 10";
    $rows = $wpdb->get_col($sql);
    if (!is_array($rows)) return [];
    return array_map('strval', $rows);
  }

  private static function model_h2_row_exists($page_id, $source) {
    if (!$page_id) return false;
    $row = self::get_h2_for_page($page_id, $source);
    return !empty($row);
  }

  private static function get_model_posts_sample($post_types, $source) {
    $types = self::csv_list($post_types);
    if (empty($types)) return [];

    $posts = get_posts([
      'post_type' => $types,
      'post_status' => 'publish',
      'posts_per_page' => 10,
      'orderby' => 'ID',
      'order' => 'DESC',
      'fields' => 'ids',
      'suppress_filters' => true,
      'no_found_rows' => true,
    ]);

    if (!is_array($posts) || empty($posts)) return [];

    $rows = [];
    foreach ($posts as $pid) {
      $page_id = self::resolve_id_for_post($pid, 'page');
      $rows[] = [
        'post_id' => intval($pid),
        'post_name' => (string)get_post_field('post_name', $pid),
        'page_id' => $page_id,
        'has_csv' => self::model_h2_row_exists($page_id, $source),
      ];
    }
    return $rows;
  }

  private static function get_lookup_result($source) {
    $post_id = isset($_GET['lookup_post_id']) ? absint($_GET['lookup_post_id']) : 0;
    if ($post_id <= 0) return null;

    $nonce = $_GET['tmwseo_csv_lookup_nonce'] ?? '';
    if (!wp_verify_nonce($nonce, 'tmwseo_csv_diag_lookup')) {
      return ['error' => 'Invalid lookup nonce. Please try again.'];
    }

    $page_id = self::resolve_id_for_post($post_id, 'page');
    $has_csv = self::model_h2_row_exists($page_id, $source);

    return [
      'post_id' => $post_id,
      'page_id' => $page_id,
      'has_csv' => $has_csv,
    ];
  }

  private static function assign_ids_to_posts($ids, $post_ids, $meta_key) {
    $assigned = 0;
    $index = 0;
    $total = count($ids);

    foreach ($post_ids as $pid) {
      if ($index >= $total) break;
      if (metadata_exists('post', $pid, $meta_key)) continue;
      $value = $ids[$index] ?? '';
      if ($value === '') continue;
      update_post_meta($pid, $meta_key, $value);
      $assigned++;
      $index++;
    }

    return ['assigned' => $assigned];
  }

  /** -------------------------
   * Content block injection
   * ------------------------ */
  private static function build_h2_block_html($post_id, $h2s) {
    $term_links = self::get_term_links_html($post_id);
    $intro = self::build_safe_intro_sentence($post_id);

    $html  = "\n<!-- TMWSEO:CSV:START -->\n";
    $html .= "<section class=\"tmwseo-csv-block\">\n";
    $html .= "<p>" . esc_html($intro) . "</p>\n";
    foreach ($h2s as $h2) {
      $html .= "<h2>" . esc_html($h2) . "</h2>\n";
      $html .= "<p>" . esc_html(self::build_h2_paragraph($h2)) . "</p>\n";
    }
    if ($term_links) {
      $html .= "<div class=\"tmwseo-related\">\n<strong>Explore more:</strong>\n<ul>$term_links</ul>\n</div>\n";
    }
    $html .= "</section>\n";
    $html .= "<!-- TMWSEO:CSV:END -->\n";
    return $html;
  }

  private static function replace_or_append_block($content, $new_block) {
    $content = (string)$content;
    $pattern = '/<!--\s*TMWSEO:CSV:START\s*-->.*?<!--\s*TMWSEO:CSV:END\s*-->/s';

    if (preg_match($pattern, $content)) {
      return preg_replace($pattern, $new_block, $content, 1);
    }
    return $content . "\n" . $new_block;
  }

  private static function get_term_links_html($post_id) {
    $terms = get_the_terms($post_id, 'category');
    if (!is_array($terms) || empty($terms)) return '';
    $items = [];
    foreach ($terms as $t) {
      $url = get_term_link($t);
      if (is_wp_error($url)) continue;
      $items[] = '<li><a href="'.esc_url($url).'">'.esc_html($t->name).'</a></li>';
    }
    return implode("\n", $items);
  }

  private static function build_safe_intro_sentence($post_id) {
    $title = get_the_title($post_id);
    $title = wp_strip_all_tags((string)$title);
    $title = trim($title);
    if ($title === '') $title = 'This page';
    return $title . " brings together highlights, themes, and related links to help you explore more content.";
  }

  private static function build_h2_paragraph($h2) {
    $h2 = wp_strip_all_tags((string)$h2);
    $h2 = trim($h2);
    if ($h2 === '') return "Explore more in this section.";
    return "Explore “{$h2}” with a quick overview, key moments, and nearby pages you may also like.";
  }

  private static function build_description($seo_title, $focus_kw, $primary_row, $all_kws) {
    $tone = isset($primary_row['tone']) ? (string)$primary_row['tone'] : '';
    $cat  = isset($primary_row['category']) ? (string)$primary_row['category'] : '';

    $parts = [];
    $parts[] = $seo_title . '.';
    if ($cat) $parts[] = "Category: {$cat}.";
    if ($tone) $parts[] = "Tone: {$tone}.";
    if ($focus_kw) $parts[] = "Explore {$focus_kw} and related highlights.";
    if (!empty($all_kws)) {
      $extra = array_slice($all_kws, 1, 3);
      if (!empty($extra)) $parts[] = "Related: " . implode(', ', $extra) . '.';
    }
    $desc = implode(' ', $parts);
    // Keep it short-ish
    return self::clean_text($desc, 160);
  }

  /** -------------------------
   * Adult SEO meta + Schema
   * ------------------------ */
  public static function output_adult_rating_meta() {
    if (!is_singular()) return;
    $s = self::get_settings();
    $types = self::csv_list($s['adult_meta_post_types']);
    $pt = get_post_type();
    if (!in_array($pt, $types, true)) return;

    echo "\n" . '<meta name="rating" content="adult" />' . "\n";
  }

  public static function output_video_schema() {
    if (!is_singular()) return;

    $s = self::get_settings();
    $types = self::csv_list($s['output_video_schema_post_types']);
    $pt = get_post_type();
    if (!in_array($pt, $types, true)) return;

    global $post;
    if (!$post) return;

    $content = (string)$post->post_content;

    // Find embed script src (your format)
    if (!preg_match('#https://tpdwm\.com/embed/tbplyr/\?[^"\s]+#i', $content, $m)) {
      return;
    }
    $embedUrl = $m[0];

    $name = wp_strip_all_tags(get_the_title($post));
    $desc = get_post_meta($post->ID, 'rank_math_description', true);
    $desc = $desc ? wp_strip_all_tags((string)$desc) : self::build_safe_intro_sentence($post->ID);

    $thumb = '';
    if (has_post_thumbnail($post)) {
      $thumb = wp_get_attachment_image_url(get_post_thumbnail_id($post), 'full');
    }

    $schema = [
      '@context' => 'https://schema.org',
      '@type' => 'VideoObject',
      'name' => $name,
      'description' => $desc,
      'embedUrl' => $embedUrl,
      'uploadDate' => get_post_time('c', true, $post),
      'isFamilyFriendly' => false,
    ];
    if ($thumb) $schema['thumbnailUrl'] = $thumb;

    echo "\n<script type=\"application/ld+json\">" . wp_json_encode($schema) . "</script>\n";
  }

  /** -------------------------
   * Video Sitemap
   * ------------------------ */
  public static function rewrite_init() {
    add_rewrite_rule('^tmwseo-video-sitemap\.xml$', 'index.php?tmwseo_video_sitemap=1', 'top');

    // Add custom interval for cron
    add_filter('cron_schedules', function($schedules) {
      if (!isset($schedules['tmwseo_two_minutes'])) {
        $schedules['tmwseo_two_minutes'] = [
          'interval' => 120,
          'display' => 'Every 2 Minutes (TMWSEO)'
        ];
      }
      return $schedules;
    });
  }

  public static function maybe_output_video_sitemap() {
    if (!get_query_var('tmwseo_video_sitemap')) return;

    $s = self::get_settings();
    $types = self::csv_list($s['output_video_schema_post_types']);
    if (empty($types)) $types = ['post'];

    header('Content-Type: application/xml; charset=utf-8');

    $posts = get_posts([
      'post_type' => $types,
      'post_status' => 'publish',
      'posts_per_page' => 5000,
      'orderby' => 'ID',
      'order' => 'DESC',
      'no_found_rows' => true,
      'suppress_filters' => true,
    ]);

    echo "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    echo "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\" xmlns:video=\"http://www.google.com/schemas/sitemap-video/1.1\">\n";

    foreach ($posts as $p) {
      $url = get_permalink($p);
      if (!$url) continue;

      $content = (string)$p->post_content;
      if (!preg_match('#https://tpdwm\.com/embed/tbplyr/\?[^"\s]+#i', $content, $m)) {
        continue;
      }
      $embedUrl = $m[0];

      $title = wp_strip_all_tags(get_the_title($p));
      $desc  = get_post_meta($p->ID, 'rank_math_description', true);
      $desc  = $desc ? wp_strip_all_tags((string)$desc) : self::build_safe_intro_sentence($p->ID);

      $thumb = '';
      if (has_post_thumbnail($p)) {
        $thumb = wp_get_attachment_image_url(get_post_thumbnail_id($p), 'full');
      }
      if (!$thumb) continue; // video sitemap needs thumbnail

      echo "  <url>\n";
      echo "    <loc>" . esc_url($url) . "</loc>\n";
      echo "    <video:video>\n";
      echo "      <video:thumbnail_loc>" . esc_url($thumb) . "</video:thumbnail_loc>\n";
      echo "      <video:title>" . esc_html($title) . "</video:title>\n";
      echo "      <video:description>" . esc_html(self::clean_text($desc, 2048)) . "</video:description>\n";
      echo "      <video:player_loc allow_embed=\"yes\">" . esc_url($embedUrl) . "</video:player_loc>\n";
      echo "      <video:family_friendly>no</video:family_friendly>\n";
      echo "    </video:video>\n";
      echo "  </url>\n";
    }

    echo "</urlset>";
    exit;
  }

  /** -------------------------
   * Logging
   * ------------------------ */
  private static function log($msg) {
    $logs = get_option(self::OPT_LOGS, []);
    if (!is_array($logs)) $logs = [];
    $logs[] = '[' . gmdate('Y-m-d H:i:s') . ' UTC] ' . $msg;
    // cap
    if (count($logs) > 2500) {
      $logs = array_slice($logs, -2000);
    }
    update_option(self::OPT_LOGS, $logs, false);
  }
}

TMW_SEO_CSV_Autopilot::boot();
