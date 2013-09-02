do $$ begin

CREATE TABLE %(name)s (
  id       bigserial   PRIMARY KEY,
  enqueued timestamptz NOT NULL DEFAULT current_timestamp,
  dequeued timestamptz,
  q_name   text        NOT NULL CHECK (length(q_name) > 0),
  data     json        NOT NULL
);

end $$ language plpgsql;

drop function if exists pq_notify() cascade;

create function pq_notify() returns trigger as $$ begin
  perform pg_notify(new.q_name, '');
  return null;
end $$ language plpgsql;

create trigger pq_insert
after insert on %(name)s
for each row
execute procedure pq_notify();
