do $$ begin

CREATE TABLE IF NOT EXISTS %(name)s (
  id          bigserial    PRIMARY KEY,
  enqueued_at timestamptz  NOT NULL DEFAULT current_timestamp,
  dequeued_at timestamptz,
  expected_at timestamptz,
  schedule_at timestamptz,
  q_name      text         NOT NULL CHECK (length(q_name) > 0),
  q_key       text         NOT NULL CHECK (length(q_key) > 0),
  data        json         NOT NULL
);

-- Handle migration from version without q_key column
BEGIN
    BEGIN
        ALTER TABLE %(name)s ADD COLUMN q_key text;
        UPDATE %(name)s SET q_key = q_name;
        ALTER TABLE %(name)s ALTER COLUMN q_key SET NOT NULL;
        ALTER TABLE %(name)s ADD CHECK (length(q_key) > 0);
        EXCEPTION
            WHEN duplicate_column THEN 
                RAISE NOTICE 'column "q_key" already exists, skipping';
        END;
    END;

end $$ language plpgsql;

create index if not exists priority_idx_%(name)s on %(name)s
    (schedule_at nulls first, expected_at nulls first, q_name)
    where dequeued_at is null
          and q_name = '%(name)s';

create index if not exists priority_idx_no_%(name)s on %(name)s
    (schedule_at nulls first, expected_at nulls first, q_name)
    where dequeued_at is null
          and q_name != '%(name)s';

drop function if exists pq_notify() cascade;

create function pq_notify() returns trigger as $$ begin
  perform pg_notify(new.q_key, '');
  return null;
end $$ language plpgsql;

create trigger pq_insert
after insert on %(name)s
for each row
execute procedure pq_notify();
