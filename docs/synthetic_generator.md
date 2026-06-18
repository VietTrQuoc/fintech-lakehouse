# Synthetic Raw Data Generator

This generator creates the raw source layer for the Financial Transaction Data Lakehouse for Fraud and Risk Analytics project.

## Environment

The checked machine has Python 3.14.5 and NumPy available. The generator environment is intentionally separate:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements-gen.txt
```

Python 3.14 is newer than many data-engineering wheels. If pandas or pyarrow cannot install, use Python 3.12 for `.venv` and keep the same `requirements-gen.txt`.

## PaySim Calibration

`src/generate/calibration/paysim_params.json` contains fallback parameters so generation works before the 470MB PaySim file is downloaded. For a true calibrated run:

1. Put Kaggle credentials at `%USERPROFILE%\.kaggle\kaggle.json`, or at `data\external\kaggle\kaggle.json` if the home directory is unavailable in the current runtime.
2. Run `notebooks/00_calibrate_paysim.ipynb`.
3. Commit or keep the regenerated `src/generate/calibration/paysim_params.json`.

Only amount log-normal parameters, transaction type ratios, and PaySim fraud rates are calibrated from PaySim. Channel mix, device behavior, location behavior, and currency mix are explicit VN fintech assumptions in `src/generate/config.py`.

The PaySim fraud rate is sparse for demos, so `GEN_FRAUD_RATE` defaults to `0.007`. This is a deliberate demo lift from the observed PaySim rate, not a claim about the original dataset.

## Generate

Small smoke run:

```powershell
.\.venv\Scripts\python -m src.generate.generate_all --n-customers 1000 --n-txn 10000 --raw-dir data\smoke\raw --bad-data-dir data\smoke\bad_data_samples
```

Large default run:

```powershell
.\.venv\Scripts\python -m src.generate.generate_all
```

The large default is 20,000 customers, 2,000,000 transactions, and an inclusive date window from 2025-12-13 through 2026-06-10. Because that is a true 180-day window ending on 2026-06-10, the transaction partitions include a partial `2025-12` file plus monthly files through `2026-06`.

## Validate

```powershell
.\.venv\Scripts\python -m src.generate.validate --raw-dir data\raw --bad-data-dir data\bad_data_samples
```

The validator checks row counts, FK integrity outside the injected error manifest, fraud rate, event-to-ingestion lag, partition files, and bucket counts for quarantine-oriented corruptions.
