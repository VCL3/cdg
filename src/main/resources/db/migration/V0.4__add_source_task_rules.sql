ALTER TABLE task_rules ADD COLUMN source varchar(30);

CREATE INDEX source_idx ON task_rules (source);
CREATE INDEX status_idx ON task_rules (status);