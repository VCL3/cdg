CREATE TABLE task_stats (
	id SERIAL PRIMARY KEY,
	  task_rule_id integer references task_rules(id),
    task_run_id integer references task_runs(id),
    stats_snapshot text,
    created_at timestamp not null,
    updated_at timestamp not null,
    finished_at timestamp
)