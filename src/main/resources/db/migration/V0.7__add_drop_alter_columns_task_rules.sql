ALTER TABLE task_rules RENAME COLUMN task_params TO search_params;

-- Renaming "limit" column name to avoid potential errors in query statements as limit is a keyword in sql.
ALTER TABLE task_rules RENAME COLUMN "limit" TO max_records;

ALTER TABLE task_rules DROP COLUMN name;
ALTER TABLE task_rules DROP COLUMN task_type;

ALTER TABLE task_rules ADD COLUMN type varchar(30);
ALTER TABLE task_rules ADD COLUMN last_updated_by varchar(30);

