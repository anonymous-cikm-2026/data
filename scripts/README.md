# Script Overview

This repository ships the helper script used to rebuild the strict MIMIC ICD-10 note dataset.

- `mimic/build_mimic_icd10_dataset_2017_2019.py`: rebuilds `mimic_icd10_note_dataset_2017_2019_strict` from the upstream MIMIC hospital and note tables plus the committed ICD addenda files.

The script uses PySpark and can run through Databricks Connect when a Databricks profile and running cluster id are supplied.