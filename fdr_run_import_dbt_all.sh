#!/bin/bash
. ../../dbt-env/bin/activate
cd ../fdr-france-data-reseau
set -a ; source env.prod_stellio ; set +a
echo check : FDR_SYNC_POSTGRES_DATABASE=$FDR_SYNC_POSTGRES_DATABASE
fal run --target prod --before
dbt run --target prod
cd ../fdr-eaupotable
dbt run --target prod
