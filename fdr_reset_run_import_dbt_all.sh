#!/bin/bash
cd ../fdr-france-data-reseau
rm -rf import_state.json
./fdr_run_import_dbt_all.sh
