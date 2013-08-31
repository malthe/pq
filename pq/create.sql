do $$ begin

DROP TABLE IF EXISTS %(name)s;

CREATE TABLE %(name)s (
  id        bigserial PRIMARY KEY,
  processed bool not null DEFAULT false,
  q_name    text not null check (length(q_name) > 0),
  data      json not null,
  locked_at timestamptz
);

end $$ language plpgsql;

drop function if exists pq_notify();

create function pq_notify() returns trigger as $$ begin
  perform pg_notify(new.q_name, '');
  return null;
end $$ language plpgsql;

create trigger pq
after insert on queue
for each row
execute procedure pq_notify();

CREATE INDEX idx_qc_on_name_only_unlocked ON queue (q_name, id) WHERE locked_at IS NULL;
