# Tools assisting with migration

## legacy_metric_dependencies.py

Finds dependencies of specified objects across workspaces on a Legacy domain. This is useful to determine which objects are used within the domain.

### Usage

```bash
# Use --help command to display help in the terminal
python tools/legacy_object_dependencies.py --help
```

```bash
python tools/legacy_object_dependencies.py <input_csv> <output_csv> --object-type <metric|attribute|insight> --workspaces-csv <workspaces_csv> [--env <env_file>]
python tools/legacy_object_dependencies.py <input_csv> <output_csv> --object-type <metric|attribute|insight> --dynamic-workspace-lookup [--env <env_file>]
```

### Arguments

| Argument                   | Required | Description                                                                                                           |
| -------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------- |
| input_csv                  | Yes      | CSV file with object identifiers (one per line)                                                                       |
| output_csv                 | Yes      | Output CSV file for dependencies                                                                                      |
| --object-type              | Yes      | Object type (check help for a list of values) |
| --workspaces-csv           | \*       | CSV file with workspace IDs to check (one per line)                                                                   |
| --dynamic-workspace-lookup | \*       | Dynamically fetch all workspaces from the Legacy domain                                                                 |
| --env                      | No       | Path to `.env` file. If not provided, the script will try to load the environment variables directly from environment |

\*One of `--workspaces-csv` or `--dynamic-workspace-lookup` is required (mutually exclusive).


### Required Environment Variables

- `LEGACY_DOMAIN` - Legacy domain URL
- `LEGACY_LOGIN` - Legacy login email
- `LEGACY_PASSWORD` - Legacy password

### Examples

Using a specific list of workspaces for metrics:

```bash
python tools/legacy_metric_dependencies.py metrics.csv dependencies.csv --object-type metric --workspaces-csv workspaces.csv --env .env
```

Using dynamic workspace lookup for attributes:

```bash
python tools/legacy_metric_dependencies.py attributes.csv dependencies.csv --object-type attribute --dynamic-workspace-lookup --env .env
```

### Output

CSV with columns: `object_id`, `workspace_id`, `link`, `author`, `created`, `deprecated`, `identifier`, `summary`, `title`, `category`, `updated`, `contributor`
