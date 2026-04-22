PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sites (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_key TEXT NOT NULL UNIQUE,
  site_name TEXT NOT NULL,
  domain TEXT NOT NULL,
  timezone TEXT NOT NULL DEFAULT 'Asia/Singapore',
  webflow_site_id TEXT NOT NULL,
  webflow_collection_id TEXT NOT NULL,
  gsc_site_url TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS clusters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_key TEXT NOT NULL UNIQUE,
  cluster_name TEXT NOT NULL,
  description TEXT,
  priority INTEGER NOT NULL DEFAULT 100,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cluster_keywords (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  cluster_id INTEGER NOT NULL,
  keyword_family TEXT NOT NULL,
  keyword_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (cluster_id) REFERENCES clusters(id)
);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value_json TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS content_pages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  page_url TEXT NOT NULL UNIQUE,
  slug TEXT NOT NULL UNIQUE,
  title_current TEXT,
  h1_current TEXT,
  cluster_id INTEGER,
  page_type TEXT NOT NULL DEFAULT 'blog',
  status TEXT NOT NULL DEFAULT 'active',
  webflow_item_id TEXT,
  last_published_at TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (site_id) REFERENCES sites(id),
  FOREIGN KEY (cluster_id) REFERENCES clusters(id)
);

CREATE TABLE IF NOT EXISTS gsc_page_daily (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  date TEXT NOT NULL,
  page_url TEXT NOT NULL,
  clicks REAL NOT NULL DEFAULT 0,
  impressions REAL NOT NULL DEFAULT 0,
  ctr REAL NOT NULL DEFAULT 0,
  position REAL NOT NULL DEFAULT 0,
  data_state TEXT NOT NULL DEFAULT 'final',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(site_id, date, page_url, data_state),
  FOREIGN KEY (site_id) REFERENCES sites(id)
);

CREATE TABLE IF NOT EXISTS gsc_query_daily (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  date TEXT NOT NULL,
  query TEXT NOT NULL,
  page_url TEXT NOT NULL,
  clicks REAL NOT NULL DEFAULT 0,
  impressions REAL NOT NULL DEFAULT 0,
  ctr REAL NOT NULL DEFAULT 0,
  position REAL NOT NULL DEFAULT 0,
  data_state TEXT NOT NULL DEFAULT 'final',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(site_id, date, query, page_url, data_state),
  FOREIGN KEY (site_id) REFERENCES sites(id)
);

CREATE TABLE IF NOT EXISTS gsc_url_inspections (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  page_url TEXT NOT NULL,
  inspection_ts TEXT NOT NULL,
  coverage_state TEXT,
  indexing_state TEXT,
  last_crawl_time TEXT,
  canonical_url TEXT,
  raw_json TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (site_id) REFERENCES sites(id)
);

CREATE TABLE IF NOT EXISTS recovery_candidates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  window_start TEXT NOT NULL,
  window_end TEXT NOT NULL,
  impressions_28d REAL NOT NULL DEFAULT 0,
  clicks_28d REAL NOT NULL DEFAULT 0,
  ctr_28d REAL NOT NULL DEFAULT 0,
  position_28d REAL NOT NULL DEFAULT 0,
  top_queries_json TEXT,
  opportunity_score REAL NOT NULL DEFAULT 0,
  reason_json TEXT,
  status TEXT NOT NULL DEFAULT 'new',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (page_id) REFERENCES content_pages(id)
);

CREATE TABLE IF NOT EXISTS recovery_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  candidate_id INTEGER NOT NULL,
  priority INTEGER NOT NULL DEFAULT 100,
  status TEXT NOT NULL DEFAULT 'queued',
  attempt_no INTEGER NOT NULL DEFAULT 1,
  queued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  locked_at TEXT,
  completed_at TEXT,
  UNIQUE(page_id, candidate_id),
  FOREIGN KEY (page_id) REFERENCES content_pages(id),
  FOREIGN KEY (candidate_id) REFERENCES recovery_candidates(id)
);

CREATE TABLE IF NOT EXISTS page_versions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  version_no INTEGER NOT NULL,
  source_type TEXT NOT NULL,
  title_tag TEXT,
  meta_description TEXT,
  h1 TEXT,
  intro_html TEXT,
  body_html TEXT,
  headings_json TEXT,
  internal_links_json TEXT,
  cta_variant TEXT,
  published_at TEXT,
  webflow_item_id TEXT,
  notes_json TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(page_id, version_no),
  FOREIGN KEY (page_id) REFERENCES content_pages(id)
);

CREATE TABLE IF NOT EXISTS recovery_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  version_id INTEGER NOT NULL,
  eval_day INTEGER NOT NULL,
  clicks_before REAL NOT NULL DEFAULT 0,
  clicks_after REAL NOT NULL DEFAULT 0,
  impressions_before REAL NOT NULL DEFAULT 0,
  impressions_after REAL NOT NULL DEFAULT 0,
  ctr_before REAL NOT NULL DEFAULT 0,
  ctr_after REAL NOT NULL DEFAULT 0,
  position_before REAL NOT NULL DEFAULT 0,
  position_after REAL NOT NULL DEFAULT 0,
  query_expansion_score REAL NOT NULL DEFAULT 0,
  assisted_internal_clicks REAL NOT NULL DEFAULT 0,
  result_score REAL NOT NULL DEFAULT 0,
  decision TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(page_id, version_id, eval_day),
  FOREIGN KEY (page_id) REFERENCES content_pages(id),
  FOREIGN KEY (version_id) REFERENCES page_versions(id)
);

CREATE TABLE IF NOT EXISTS keyword_candidates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  cluster_id INTEGER NOT NULL,
  primary_keyword TEXT NOT NULL,
  secondary_keywords_json TEXT,
  source TEXT NOT NULL,
  volume REAL,
  difficulty REAL,
  intent_type TEXT NOT NULL,
  fit_score REAL NOT NULL DEFAULT 0,
  cluster_score REAL NOT NULL DEFAULT 0,
  total_score REAL NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'new',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(site_id, primary_keyword),
  FOREIGN KEY (site_id) REFERENCES sites(id),
  FOREIGN KEY (cluster_id) REFERENCES clusters(id)
);

CREATE TABLE IF NOT EXISTS article_generation_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  keyword_candidate_id INTEGER NOT NULL,
  priority INTEGER NOT NULL DEFAULT 100,
  status TEXT NOT NULL DEFAULT 'queued',
  attempt_no INTEGER NOT NULL DEFAULT 1,
  queued_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  locked_at TEXT,
  completed_at TEXT,
  UNIQUE(keyword_candidate_id),
  FOREIGN KEY (keyword_candidate_id) REFERENCES keyword_candidates(id)
);

CREATE TABLE IF NOT EXISTS article_briefs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  queue_id INTEGER NOT NULL UNIQUE,
  cluster_id INTEGER NOT NULL,
  primary_keyword TEXT NOT NULL,
  secondary_keywords_json TEXT,
  search_intent TEXT,
  article_angle TEXT,
  title_options_json TEXT,
  outline_json TEXT,
  internal_links_json TEXT,
  cta_angle TEXT,
  brief_text TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (queue_id) REFERENCES article_generation_queue(id),
  FOREIGN KEY (cluster_id) REFERENCES clusters(id)
);

CREATE TABLE IF NOT EXISTS article_drafts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  queue_id INTEGER NOT NULL UNIQUE,
  title_tag TEXT NOT NULL,
  meta_description TEXT,
  slug TEXT NOT NULL UNIQUE,
  h1 TEXT NOT NULL,
  excerpt TEXT,
  body_html TEXT NOT NULL,
  faq_json TEXT,
  schema_json TEXT,
  image_prompt TEXT,
  quality_score REAL NOT NULL DEFAULT 0,
  validation_json TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (queue_id) REFERENCES article_generation_queue(id)
);

CREATE TABLE IF NOT EXISTS internal_link_targets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  page_id INTEGER NOT NULL,
  target_page_id INTEGER NOT NULL,
  anchor_text TEXT NOT NULL,
  link_type TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (page_id) REFERENCES content_pages(id),
  FOREIGN KEY (target_page_id) REFERENCES content_pages(id)
);

CREATE TABLE IF NOT EXISTS image_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_type TEXT NOT NULL,
  source_id INTEGER NOT NULL,
  prompt TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  attempt_no INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS generated_images (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  queue_id INTEGER NOT NULL UNIQUE,
  local_path TEXT NOT NULL,
  alt_text TEXT,
  status TEXT NOT NULL DEFAULT 'generated',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (queue_id) REFERENCES image_queue(id)
);

CREATE TABLE IF NOT EXISTS webflow_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  page_type TEXT NOT NULL,
  source_id INTEGER NOT NULL,
  collection_id TEXT NOT NULL,
  item_id TEXT,
  slug TEXT NOT NULL,
  is_draft INTEGER NOT NULL DEFAULT 1,
  last_published TEXT,
  sync_status TEXT NOT NULL DEFAULT 'pending',
  last_sync_at TEXT,
  payload_hash TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(page_type, source_id)
);

CREATE TABLE IF NOT EXISTS publish_plan (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_type TEXT NOT NULL,
  source_id INTEGER NOT NULL,
  planned_publish_ts_utc TEXT NOT NULL,
  actual_publish_ts_utc TEXT,
  status TEXT NOT NULL DEFAULT 'planned',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS notifications_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT NOT NULL,
  event_key TEXT NOT NULL,
  channel TEXT NOT NULL,
  sent_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status TEXT NOT NULL DEFAULT 'sent',
  UNIQUE(event_type, event_key, channel)
);

CREATE TABLE IF NOT EXISTS runs_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_name TEXT NOT NULL,
  started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at TEXT,
  status TEXT NOT NULL DEFAULT 'running',
  items_processed INTEGER NOT NULL DEFAULT 0,
  error_count INTEGER NOT NULL DEFAULT 0,
  cost_json TEXT
);

CREATE TABLE IF NOT EXISTS error_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_name TEXT NOT NULL,
  source_type TEXT,
  source_id INTEGER,
  error_type TEXT NOT NULL,
  error_message TEXT NOT NULL,
  raw_json TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
