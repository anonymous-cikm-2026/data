# CIKM Submission 2026 Artifacts

This repository keeps the paper artifacts in one place: the final approved legal dataset, the public reference manuals used by the paper, and the code and notes needed to rebuild the MIMIC ICD-10 2017-2019 strict table.

## What is in this repository

- `data/final-approved-200/`: the final legally approved 200-point release committed as a CSV file.
- `data/reference-manuals/`: public ICD and legal reference manuals used by the paper.
- `data/mimic/`: local summaries and notes related to rebuilding the MIMIC strict table.
- `manuals/`: project documentation for the data layout and MIMIC build path.
- `scripts/`: helper scripts used to rebuild the MIMIC strict table.

## Legal and review note

Only the final approved 200-point package should be committed under `data/final-approved-200/`.
Only public reference manuals are committed under `data/reference-manuals/`; extracted working artifacts, logs, and source-project traces are intentionally excluded to preserve anonymity.
Raw MIMIC data must not be redistributed in this repository. The MIMIC side of this repo documents and scripts how to rebuild the strict 2017-2019 ICD-10 note dataset from credentialed upstream tables.

## Quick start

1. Inspect the committed approved release in `data/final-approved-200/`.
2. Inspect the public manuals under `data/reference-manuals/`.
3. Read the project documents:
	- `manuals/data-manual.md`
	- `manuals/mimic-manual.md`
4. If you need to rebuild the MIMIC ICD-10 2017-2019 strict table, follow `manuals/mimic-manual.md` and run `scripts/mimic/build_mimic_icd10_dataset_2017_2019.py`.

## Repository layout

```text
.
|-- data/
|   |-- README.md
|   |-- final-approved-200/
|   |   |-- federal_sentencing_legal_final_dataset_approved.csv
|   |   `-- README.md
|   |-- reference-manuals/
|   |   |-- README.md
|   |   |-- ICD-addendums/
|   |   |-- ICD-2019-manual/
|   |   `-- legal/
|   `-- mimic/
|-- manuals/
|   |-- data-manual.md
|   `-- mimic-manual.md
`-- scripts/
	|-- README.md
	`-- mimic/
		|-- README.md
		`-- build_mimic_icd10_dataset_2017_2019.py
```
