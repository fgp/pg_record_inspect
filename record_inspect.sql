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
	'$libdir/record_inspect', 'record_inspect_fieldinfos'
language 'C' strict immutable;
comment on function fieldinfos(record) is
'Returns an array of <fieldinfo>s describing the record''s fields';

create function fieldvalue(record, field name, defval anyelement, coerce boolean default true) returns anyelement as
	'$libdir/record_inspect', 'record_inspect_fieldvalue'
language 'C' immutable;
comment on function fieldvalue(record, name, anyelement, boolean)  is
'Returns the value of the field <field>, or <defval> should the value be null.
If <coerce> is true, the value is coerced to <defval>''s type if possible,
otherwise an error is raised if the field''s type and <defval>''s type differ.';

create function fieldvalues(record, defval anyelement, coerce boolean default true) returns anyarray as
	'$libdir/record_inspect', 'record_inspect_fieldvalues'
language 'C' immutable;
comment on  function fieldvalues(record, anyelement, boolean) is
'Returns an array containing values of the record'' fields.
NULL values are replaced by <defval>. If <coerce> is false, only
the fields with the same type as <defval> are considered. Otherwise,
the field'' values are coerced if possible, or an error is raised if not.';

commit;
