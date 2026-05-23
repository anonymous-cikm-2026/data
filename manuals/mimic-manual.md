# MIMIC Manual

This repository does not redistribute raw MIMIC data. Instead, it provides the references and local script needed to rebuild the strict ICD-10 note table used in the paper: `usdo_aa_catalog.research_tam_datasets.mimic_icd10_note_dataset_2017_2019_strict`.

## Official resources

- MIMIC-IV: https://physionet.org/content/mimiciv/
- MIMIC-IV Demo: https://physionet.org/content/mimiciv-demo/
- MIMIC-IV-ED: https://physionet.org/content/mimic-iv-ed/
- MIMIC Code Repository: https://github.com/MIT-LCP/mimic-code/
- PhysioNet credentialing: https://physionet.org/settings/credentialing/

## Source tables

The build uses these Databricks tables:

- `usdo_aa_catalog.research_tam_datasets.mimic_hosp_admissions`
- `usdo_aa_catalog.research_tam_datasets.mimic_hosp_diagnoses_icd`
- `usdo_aa_catalog.research_tam_datasets.mimic_hosp_patients`
- `usdo_aa_catalog.research_tam_datasets.mimic_note_discharge`

It also uses the public ICD addenda files committed under `data/reference-manuals/ICD-addendums/`.

## Build logic

The strict 2017-2019 table is built as follows:

1. Start from all MIMIC hospital diagnosis rows with `icd_version = 10`.
2. Normalize ICD codes by removing periods and uppercasing.
3. Parse the 2018-2020 ICD addenda files and collect every code touched by an add, delete, or revise event.
4. Exclude any admission if any assigned ICD-10 diagnosis code appears in that touched-code set.
5. Recover a real discharge-year range using `anchor_year`, `anchor_year_group`, and the shifted `dischtime`.
6. Keep only admissions whose full real discharge-year range lies within 2017-2019.
7. Join discharge notes and retain one discharge-note-backed row per surviving admission.
8. Aggregate the remaining ICD-10 codes per admission in sequence order into `output_icd_codes`.
9. Write the final table to Delta.

## Build command

Run the build with Databricks Connect from a machine that has access to the source tables:

```bash
python scripts/mimic/build_mimic_icd10_dataset_2017_2019.py ^
  --profile <databricks-profile> ^
  --cluster-id <running-cluster-id>
```

To validate counts without writing the table, add `--skip-write`:

```bash
python scripts/mimic/build_mimic_icd10_dataset_2017_2019.py ^
  --profile <databricks-profile> ^
  --cluster-id <running-cluster-id> ^
  --skip-write
```

## Output contract

The script targets `usdo_aa_catalog.research_tam_datasets.mimic_icd10_note_dataset_2017_2019_strict` by default and writes a local JSON summary to `data/mimic/mimic_icd10_note_dataset_2017_2019_strict_summary.json`.