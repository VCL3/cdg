ALTER TABLE task_rules ADD COLUMN creator_id varchar(30);
ALTER TABLE task_rules ADD COLUMN client_id varchar(30) NOT NULL default 'dora';
ALTER TABLE task_rules ALTER COLUMN client_id DROP DEFAULT;