CREATE TABLE task_rules (
    id SERIAL PRIMARY KEY,
    name varchar(100) not null,
    task_type varchar(100) not null,
    status varchar(100) not null,
    task_params text,
    recurrence text,
    next_schedule_at timestamp DEFAULT current_timestamp,
    created_at timestamp DEFAULT current_timestamp,
    updated_at timestamp DEFAULT current_timestamp
);

CREATE INDEX rule_schedule_idx ON task_rules (status, next_schedule_at);