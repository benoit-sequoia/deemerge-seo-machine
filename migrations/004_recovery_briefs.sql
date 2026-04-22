CREATE TABLE IF NOT EXISTS recovery_briefs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  queue_id INTEGER NOT NULL UNIQUE,
  page_id INTEGER NOT NULL,
  primary_query TEXT,
  secondary_queries_json TEXT,
  suggested_title TEXT,
  suggested_h1 TEXT,
  rewrite_focus_json TEXT,
  brief_text TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (queue_id) REFERENCES recovery_queue(id),
  FOREIGN KEY (page_id) REFERENCES content_pages(id)
);
