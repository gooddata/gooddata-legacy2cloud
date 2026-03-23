# Tools assisting with migration

## platform_metric_dependencies.py

Finds dependencies of specified objects across workspaces on a Platform domain. This is useful to determine which objects are used within the domain.

### Usage

```bash
python tools/platform_metric_dependencies.py <input_csv> <output_csv> --object-type <metric|attribute> --workspaces-csv <workspaces_csv> [--env <env_file>]
python tools/platform_metric_dependencies.py <input_csv> <output_csv> --object-type <metric|attribute> --dynamic-workspace-lookup [--env <env_file>]
```

### Arguments

| Argument                   | Required | Description                                                                                                           |
| -------------------------- | -------- | --------------------------------------------------------------------------------------------------------------------- |
| input_csv                  | Yes      | CSV file with object identifiers (one per line)                                                                       |
| output_csv                 | Yes      | Output CSV file for dependencies                                                                                      |
| --object-type              | Yes      | Object type (check help for a list of values) |
| --workspaces-csv           | \*       | CSV file with workspace IDs to check (one per line)                                                                   |
| --dynamic-workspace-lookup | \*       | Dynamically fetch all workspaces from the Platform domain                                                                 |
| --env                      | No       | Path to `.env` file. If not provided, the script will try to load the environment variables directly from environment |

\*One of `--workspaces-csv` or `--dynamic-workspace-lookup` is required (mutually exclusive).


### Required Environment Variables

- `PLATFORM_DOMAIN` - Platform domain URL
- `PLATFORM_LOGIN` - Platform login email
- `PLATFORM_PASSWORD` - Platform password

### Examples

Using a specific list of workspaces for metrics:

```bash
python tools/platform_metric_dependencies.py metrics.csv dependencies.csv --object-type metric --workspaces-csv workspaces.csv --env .env
```

Using dynamic workspace lookup for attributes:

```bash
python tools/platform_metric_dependencies.py attributes.csv dependencies.csv --object-type attribute --dynamic-workspace-lookup --env .env
```

### Output

CSV with columns: `object_id`, `workspace_id`, `link`, `author`, `created`, `deprecated`, `identifier`, `summary`, `title`, `category`, `updated`, `contributor`
