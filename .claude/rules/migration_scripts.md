# GoodData Migration Toolkit - Migration Scripts

## Migration Order (Critical)

```
LDM → Metrics → Insights → Dashboards → Reports → Pixel Perfect Dashboards → Scheduled Exports
```

Dependency chain:
- **Metrics** require `ldm_mappings.csv`
- **Insights** require `metric_mappings.csv`, `ldm_mappings.csv`
- **Dashboards** require `insight_mappings.csv`, `metric_mappings.csv`, `ldm_mappings.csv`
- **Reports** require `ldm_mappings.csv`, `metric_mappings.csv`
- **Scheduled Exports** require `insight_mappings.csv`, `dashboard_mappings.csv`
- **Dashboard Permissions** require `dashboard_mappings.csv`

## Inputs

Script input is defined by configuration objects in `src/gooddata_legacy2cloud/config/`

## Output Files

- `{prefix}{type}_mappings.csv` — Legacy identifier, Legacy ID, Cloud ID
- `{prefix}{type}_logs.log` — Transformation details, warnings, errors
- `{prefix}cloud_failed_{type}.json` — Objects that failed to create
- `{prefix}cloud_skipped_{type}.json` — Objects skipped (already exist)
- `{prefix}legacy_{type}.json` / `{prefix}cloud_{type}.json` — Dumps (optional)
