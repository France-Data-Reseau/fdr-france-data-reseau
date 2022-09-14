'''

How to use it :
configure it to be run by fal on a .jsonld.sql view of an existing .sql to ngsild-ize
Example :
models:
  - name: georef-france-commune.jsonld
    description: les communes du COG par ODS
    config:
      meta:
        type: Commune
        info: schema={{ target.schema }}
        select:
        fal:
          scripts:
            after:
              - scripts/ngsild/to_ngsild.py

then :
fal run
or
fal run --select georef-france-commune.jsonld
'''

from ckanapi import RemoteCKAN, ValidationError, NotAuthorized
import tempfile
from os import remove, getenv
from datetime import datetime

# for json output
import json


externally_defined_field_names = ['id', 'type', '@context', 'location']

model_df = execute_sql('select * from {{ ref("' + context.current_model.name + '") }} limit 1')

model_df['location'] = model_df['location'].apply(lambda v: { "type" : "GeoProperty", "value" : json.loads(v) })

# value or "" because if null error Property has an instance without a value

for field_name in model_df.columns:
    if field_name not in externally_defined_field_names:
        model_df[field_name] = model_df[field_name].apply(lambda v: { "type" : "Property", "value" : v or "" }).to_frame()

# move _name under _code :
for field_name in model_df.columns:
    if field_name not in externally_defined_field_names:
        name_field_name = field_name[:-5] + '_name'
        if field_name.endswith('_code') and field_name not in ['com_code'] and name_field_name in model_df.columns:
            model_df[field_name] = model_df.apply(lambda row: { **row[field_name], name_field_name : row[name_field_name] }, axis=1).to_frame()

model_without_code_names_df = model_df.drop(filter(lambda field_name: field_name.endswith('_name') and field_name not in ['com_name'], model_df.columns), axis=1)

jsonldPayload = model_without_code_names_df.to_dict(orient='records')
print(json.dumps(jsonldPayload, indent=4))

FDR_CAS_USAGE = "apcom"
jsonldType = "Commune"
uri_use_case_prefix = "https://ontology.francedatareseau.fr/" + FDR_CAS_USAGE
jsonldContext = { "@context" : { **{ jsonldType : uri_use_case_prefix + "#" + jsonldType},
                                 **{ field_name : uri_use_case_prefix + "/" + jsonldType.lower() + "#" + field_name for field_name in model_df.columns if field_name not in externally_defined_field_names }}
}
print(json.dumps(jsonldContext, indent=4))
#compoundJsonldContext = { "@context" :  [
#    { field_name :"https://raw.githubusercontent.com/france-data-reseau/fdr-data-models/master/apcom/jsonld-contexts/"  field_name } for field_name in model_df.columns
#]}
#print(json.dumps(compoundJsonldContext, indent=4))