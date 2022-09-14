{#
Parsing of a priori (made-up), covering examples of the definition / interface.
Examples have to be **as representative** of all possible data as possible because they are also the basis of the definition.
For instance, for a commune INSEE id field, they should also include a non-integer value such as 2A035 (Belvédère-Campomoro).
Methodology :
1. copy the first line(s) from the specification document
2. add line(s) to contain further values for until they are covering for all columns
3. NB. examples specific to each source type are provided in _source_example along their implementation (for which they are covering)

TODO can't be replaced by from_csv because is the actual definition, BUT could be by guided by metamodel !
{{ eaupot_def_reparations_from_csv(ref(model.name[:-4])) }}

#}

{{
  config(
    materialized="view"
  )
}}

select * from (values
    (ST_GeomFROMText('POLYGON((3.8 48.5, 3.8 48.6, 3.9 48.6, 3.8 48.5))', 2154)),
    (ST_GeomFROMText('POLYGON((3.6 48.3, 3.6 48.4, 3.7 48.4, 3.6 48.3))', 2154))
) s(geom)
