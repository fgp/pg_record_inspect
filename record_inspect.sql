begin;

drop schema if exists record_inspect cascade;
create schema record_inspect;
set search_path=record_inspect,pg_catalog;

create type fieldinfo as (
	fieldname		name,
	fieldtype		regtype,
	fieldtypemod	int
);

create function fieldinfos(record) returns fieldinfo[] as
	'/Users/fgp/Versioned/fgp/projects/pg_record_support/record_inspect', 'record_inspect_fieldinfos'
language 'C' strict immutable;

create function fieldvalue(record, field name, defval anyelement, coerce boolean default true) returns anyelement as
	'/Users/fgp/Versioned/fgp/projects/pg_record_support/record_inspect', 'record_inspect_fieldvalue'
language 'C' immutable;

create function fieldvalues(record, defval anyelement, coerce boolean default true) returns anyarray as
	'/Users/fgp/Versioned/fgp/projects/pg_record_support/record_inspect', 'record_inspect_fieldvalues'
language 'C' immutable;

commit;
