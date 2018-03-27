CREATE TABLE task_runs (
	id SERIAL PRIMARY KEY,
    task_rule_id integer references task_rules(id),
    status varchar(100) not null,
    state_snapshot text,
    created_at timestamp not null,
    updated_at timestamp not null 
)