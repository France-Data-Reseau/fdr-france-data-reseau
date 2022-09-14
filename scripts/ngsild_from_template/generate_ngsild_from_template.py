'''
POC generating NGSI-LD contexts and payloads using its own jinja2 engine & templates.

OBSOLETE : in any way, using DBT's templates is better (see to_ngsild.py)
can be reused though : the independent startup of fal

Aims at generating NGSILD JSONLD (and context) using separate jinja2 templates (not DBT's)
For now has hardcoded parameters

How to use it :
cd fdr-francedatareseau/scripts/ngsild_from_template
python generate_ngsild_from_template.py
'''

import jinja2

# TODO better elsewhere / deduce :
fdr_cas_usages = {
    'apcom' : {
        'use_case_prefix' : 'apcom',
        'schema' : 'appuiscommuns'
    },
    'sdirve' : {
        'use_case_prefix' : 'sdirve',
        'schema' : 'sdirve'
    },
    'eaupotable' : {
        'use_case_prefix' : 'eaupot',
        'schema' : 'eaupotable'
    },
    'eclairage_public' : {
        'use_case_prefix' : 'eclpub',
        'schema' : 'eclairage_public'
    },
    'france-data-reseau' : {
        'use_case_prefix' : 'fdr',
        'schema' : 'france-data-reseau'
    }
}

from fal import FalDbt
faldbt = FalDbt(profiles_dir="~/.dbt", project_dir="../..")


def generate_ngsild(def_model_name):
    model_df = faldbt.ref(def_model_name)
    model_df.columns

    templateLoader = jinja2.FileSystemLoader(searchpath="./")
    templateEnv = jinja2.Environment(loader=templateLoader)
    TEMPLATE_FILE = "jsonld-context_template.jsonld"
    template = templateEnv.get_template(TEMPLATE_FILE)

    FDR_CAS_USAGE = 'apcom'
    use_case_prefix = fdr_cas_usages[FDR_CAS_USAGE]['use_case_prefix']
    def map_field_name(field_name) :
        #if field_name geom
        return field_name[field_name.startswith(use_case_prefix) and len(use_case_prefix):]

    type = 'supportaerien' # def_model_name.
    jinja_vars = {
        'map_field_name' : map_field_name,
        'FDR_CAS_USAGE' : FDR_CAS_USAGE,
        'type' : type,
        'model_df' : model_df
    }
    outputText = template.render(jinja_vars)  # this is where to put args to the template renderer

    print(outputText)
    #with open(import_state_file, 'w') as f:
    #    json.dump(import_state, f, indent=4)


generate_ngsild('apcom_def_supportaerien_example')