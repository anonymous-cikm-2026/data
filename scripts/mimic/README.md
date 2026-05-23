# MIMIC Scripts

The helper in this directory rebuilds the strict ICD-10 note table used in the paper.

- `build_mimic_icd10_dataset_2017_2019.py`: rebuilds `usdo_aa_catalog.research_tam_datasets.mimic_icd10_note_dataset_2017_2019_strict` from the MIMIC admissions, diagnoses, patients, and discharge-note tables, while excluding admissions that touch ICD codes changed by the 2018-2020 addenda.