\i record_inspect.sql

create type ri_test_type as (s varchar, i int);
create table ri_test_table (r ri_test_type, f char);

insert into ri_test_table (r, f) values (row('foo', 1), 's');
insert into ri_test_table (r, f) values (row('bar', 1), 'i');

-- Get field infos for anonymous record type
select record_inspect.fieldinfos(row('Hello, World', 'foobar'::varchar(3), i::text, i, i::bigint))
	from generate_series(1,2) as i;

-- Get field infos for record in ri_test_table
select record_inspect.fieldinfos(ri_test_table) from ri_test_table;

-- Get field specified by 'f' column
select record_inspect.fieldvalue(ri_test_table.r, ri_test_table.f, NULL::text) from ri_test_table;

-- Get field value as varchar(4) to check typmod handling
-- NOTE: Currently does not work!
select record_inspect.fieldvalue(row('Hello, World'), 'f1', NULL::varchar(4));

-- Get field values for anonymous record type as regclass[]
with rs as (values ('{pg_class}', array['pg_attribute']))
select record_inspect.fieldvalue(r, 'column' || i, NULL::regclass[])
	from rs as r,
	     generate_series(1,2) as i;

-- Get field values for anonymous record type as regclass[]
-- Should fail, since date[] cannot be coerced to regclass[]
with rs as (values ('{pg_class}', array['pg_attribute'], array[to_timestamp('2009-12-20', 'YYYY-MM-DD')]))
select record_inspect.fieldvalue(r, 'column' || i, NULL::regclass[])
	from rs as r,
	     generate_series(1,3) as i;

-- Get field values of all integer fields for anonymous record type
with rs as (values ('0', 1, '0'::text, 2, 3, 4::bigint))
select record_inspect.fieldvalues(r, NULL::int, false)
	from rs as r;

-- Get field values of all fields as text for anonymous record type
with rs as (values ('1', 2, '3'::text, 4, 5, 6::bigint))
select record_inspect.fieldvalues(r, NULL::text)
	from rs as r;

-- Get field values of all fields as int for anonymous record type.
-- Should fail, since dates cannot be coerced to ints
with rs as (values ('1', 2, '3'::text, 4, 5, 6::bigint, to_timestamp('2009-12-20', 'YYYY-MM-DD')))
select record_inspect.fieldvalues(r, NULL::int)
	from rs as r;

-- Trigger to output old record version as xml.
create function log_old_xml() returns trigger as $body$
declare
	v_xml text;
begin
	v_xml := array_to_string(array(
		select
			'<' || f.fieldname || ' '
				'type=' || quote_ident(f.fieldtype::text) ||
				coalesce(
					' mod="' || replace(f.fieldtypemod, '"', E'\\"') || '"',
					''
				) ||
			'>' ||
			replace(replace(
				record_inspect.fieldvalue(OLD, f.fieldname, NULL::text),
				'<', '&lt;'), '>', '&gt;'
			) ||
			'<' || f.fieldname || '/>'
		from unnest(record_inspect.fieldinfos(OLD)) f
	), '');
	raise notice '% %', TG_OP, v_xml;
	
	RETURN NULL;
end;
$body$ language plpgsql stable;
create table t(s varchar(20), i int, d date);
create trigger t_log_old_xml after update or delete on t for each row execute procedure log_old_xml();
insert into t (s,i,d) values ('One', 1, to_date('2009-12-20', 'YYYY-MM-DD'));
insert into t (s,i,d) values ('Two', 2, to_date('2009-12-20', 'YYYY-MM-DD'));
insert into t (s,i,d) values ('Three', 3, to_date('2009-12-20', 'YYYY-MM-DD'));
delete from t where i < 3;
alter table t drop column i;
delete from t;
