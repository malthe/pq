do $$ begin

CREATE TABLE %(name)s (
  id          bigserial    PRIMARY KEY,
  enqueued_at timestamptz  NOT NULL DEFAULT current_timestamp,
  dequeued_at timestamptz,
  expected_at timestamptz,
  schedule_at timestamptz,
  q_name      text         NOT NULL CHECK (length(q_name) > 0),
  data        json         NOT NULL
);

end $$ language plpgsql;

create index priority_idx_%(name)s on %(name)s
    (schedule_at nulls first, expected_at nulls first, q_name)
    where dequeued_at is null
          and q_name = '%(name)s';

create index priority_idx_no_%(name)s on %(name)s
    (schedule_at nulls first, expected_at nulls first, q_name)
    where dequeued_at is null
          and q_name != '%(name)s';

drop function if exists pq_notify() cascade;

create function pq_notify() returns trigger as $$ begin
  perform pg_notify(new.q_name, '');
  return null;
end $$ language plpgsql;

create trigger pq_insert
after insert on %(name)s
for each row
execute procedure pq_notify();
