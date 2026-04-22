INSERT OR IGNORE INTO clusters (cluster_key, cluster_name, description, priority)
VALUES
('shared_inbox', 'Shared inbox and team inbox', 'Shared inbox, team inbox, collaborative inbox, Gmail shared inbox, Outlook shared mailbox', 10),
('gmail_slack_coordination', 'Gmail Slack Outlook coordination', 'Gmail Slack workflow, Outlook Slack workflow, email to Slack coordination', 20),
('email_triage', 'Email triage prioritization assignment', 'Email triage, prioritization, assignment, missed follow ups, inbox organization', 30),
('alternatives', 'Alternatives and comparison', 'Front alternatives, Hiver alternatives, team inbox software, shared inbox versus help desk', 40);
