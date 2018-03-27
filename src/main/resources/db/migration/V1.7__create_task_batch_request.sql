CREATE TABLE task_batch_request (
    id SERIAL PRIMARY KEY,
    source varchar(100) not null,
    status varchar(100) not null,
    fetch_request varchar(5000) not null,
    created_at timestamp DEFAULT current_timestamp,
    updated_at timestamp DEFAULT current_timestamp
);

CREATE INDEX batch_status_idx ON task_batch_request (status);
