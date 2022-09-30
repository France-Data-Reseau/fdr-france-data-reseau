#!/bin/bash
. ../../dbt-env/bin/activate
cd ../fdr-france-data-reseau

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
dbt run --target prod
cd ../fdr-eaupotable
dbt run --target prod
