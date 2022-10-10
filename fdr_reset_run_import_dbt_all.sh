#!/bin/bash
cd ../fdr-france-data-reseau
rm -rf import_state.json

# TOOD better below : same as regular only with --full-refresh

# patch env vars set when run by Nifi :
# (else error permission denied when running Docker for ogr2ogr)
set -a
unset SUDO_USER
unset SUDO_UID
unset SUDO_GID
set +a

set -a ; source env.prod_stellio ; set +a
echo check :
FDR_SYNC_POSTGRES_DATABASE=$FDR_SYNC_POSTGRES_DATABASE
SUDO_USER=SUDO_USER
fal run --target prod --before
dbt deps
dbt seed --target prod --full-refresh
dbt run --target prod --full-refresh
cd ../fdr-eaupotable
dbt deps
dbt seed --target prod --full-refresh
dbt run --target prod --full-refresh
cd ../fdr-appuiscommuns
dbt deps
dbt seed --target prod --full-refresh
dbt run --target prod --full-refresh
