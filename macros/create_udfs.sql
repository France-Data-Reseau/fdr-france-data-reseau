{#
Defines useful UDFs :
- lenient cast / conversion, from text (so if may not be text, their use must be conditioned in dedicated macros such as to_numeric_or_null.sql)

To use them, they have to be prefixed by the current schema (because they are created here, else no rights),
and arguments must have the exact right type, ex. :
{{ schema }}.to_date_or_null("{{ sourceFieldPrefix }}DateConstruction__s", 'YYYY/MM/DD HH24:mi:ss.SSS'::text)

Must be called as a DBT pre-hook.
Big UDFs should be defined in their own files and only referenced there.
Follows the principles described at :
https://discourse.getdbt.com/t/using-dbt-to-manage-user-defined-functions/18
#}

{% macro create_udfs() %}

SELECT pg_catalog.set_config('search_path', '{{target.schema}}', false); -- because no rights for create schema if not exists {{target.schema}};

create or replace function to_date_or_null (s text, fmts VARIADIC text[])
  returns date
as $$
DECLARE
  fmt text;
  d date;
begin
  IF s is NULL or length(trim(s)) = 0 THEN
    return NULL;
  END IF;
  FOREACH fmt IN ARRAY fmts
  LOOP
    begin
      d := to_date(s, fmt);
      IF d IS NOT NULL THEN
        return d;
      END IF;
    exception
      when others then -- do nothing, loop
    end;
  END LOOP;
  return NULL;
exception
  when others then return null;
end;
$$ language plpgsql;

-- NOT USED in case where checking PostGIS types really can't be done in DBT, so would have to be done in PgSQL :
-- inspired by https://dzone.com/articles/polymorphism-in-sql-part-one-anyelement-and-anyarr
create or replace function __geojson_to_geometry_or_null (val ANYELEMENT, srid integer)
  returns public.geometry -- else error can't find type geometry
as $$
declare
  val_type constant regtype := pg_typeof(val);
begin
  case val_type
      when pg_typeof(null::public.geometry) then
        return val;
      else
        return ST_Transform(ST_GeomFromGeoJSON(s), srid);
  end case;
exception
  when others then return null;
end;
$$ language plpgsql;
create or replace function __wkt_to_geometry_or_null (val ANYELEMENT, srid integer)
  returns public.geometry -- else error can't find type geometry
as $$
declare
  val_type constant regtype := pg_typeof(val);
begin
  case val_type
      when pg_typeof(null::public.geometry) then
        return val;
      else
        return ST_GeomFROMText(s, srid);
  end case;
exception
  when others then return null;
end;
$$ language plpgsql;

-- NOO NOW TODO LATER when able to check geometry type in DBT :
create or replace function geojson_to_geometry_or_null (s text, srid integer)
  returns public.geometry -- else error can't find type geometry
as $$
begin
  return ST_Transform(ST_GeomFromGeoJSON(s), srid);
exception
  when others then return null;
end;
$$ language plpgsql;
create or replace function wkt_to_geometry_or_null (s text, srid integer)
  returns public.geometry -- else error can't find type geometry
as $$
begin
  return ST_GeomFROMText(s, srid);
exception
  when others then return null;
end;
$$ language plpgsql;

create or replace function to_numeric_or_null (s text)
  returns numeric
as $$
begin
  return cast(s as numeric);
exception
  when others then return null;
end;
$$ language plpgsql;

create or replace function to_decimal_or_null (s text)
  returns decimal
as $$
begin
  return cast(s as decimal);
exception
  when others then return null;
end;
$$ language plpgsql;

create or replace function to_integer_or_null (s text)
  returns integer
as $$
begin
  return cast(s as integer);
exception
  when others then return null;
end;
$$ language plpgsql;

create or replace function to_boolean_or_null (s text)
  returns boolean
as $$
begin
  return cast(s as boolean); -- incl. null
exception
  when others then
  begin
    return case lower(trim(s::text)) when 'oui' or 'yes' then true else false end; -- trim accepts null
  exception
    when others then return null;
  end;
end;
$$ language plpgsql;

create or replace function to_boolean_or_null (n numeric)
  returns boolean
as $$
begin
  return cast(n as boolean); -- incl. null
exception
  when others then return null;
end;
$$ language plpgsql;

create or replace function to_text_or_null (s text)
  returns text
as $$
begin
  return case length(trim(s)) when 0 then null else s end; -- trim accepts null
exception
  when others then return null;
end;
$$ language plpgsql;

{% endmacro %}