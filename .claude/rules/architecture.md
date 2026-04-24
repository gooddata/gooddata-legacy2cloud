# GoodData Migration Toolkit - Architecture

## Terminology

- **Legacy**: GoodData Legacy (legacy private cloud)
- **Cloud**: GoodData Cloud (modern cloud platform)

## Three-Tier Pattern

```
Migration Scripts (src/gooddata_legacy2cloud/workflows/migrate_*.py) → Builders → API Clients
```

- **Migration Scripts**: Entry points orchestrating the full migration process
- **Builders**: Transform Legacy objects to Cloud objects (e.g., `CloudMetricsBuilder`)
- **API Clients**: `src/gooddata_legacy2cloud/backends/legacy/client.py` (custom REST), `src/gooddata_legacy2cloud/backends/cloud/client.py` (SDK + custom REST)

## Mapping Files

- CSV files tracking Legacy ID → Cloud ID mappings
- Later scripts depend on mapping files produced by earlier scripts
- Format: `{prefix}{type}_mappings.csv` (e.g., `metric_mappings.csv`)

## Object Skip Behavior

- Objects already in Cloud are skipped by default (based on ID)
- Use `--overwrite-existing` to update existing objects
- Skipped IDs logged to `{prefix}cloud_skipped_{type}.json`

## Client Prefix Pattern

Enables parent/child workspace migrations. With `--client-prefix client1_`:
- Reads both default and prefixed mapping files
- Only migrates objects not in default (parent) mappings
- All output files get the prefix
