# HIP / SDC Data Pipeline

A CLI tool to perform the aggregations, suppression for HIP / SDC.

Removes any value that is below the threshold provided (reads from metadata but usually 6), and all values that could be used to infer those values.

This is calculated based on the indentation level provided in the Census Reporter metadata for all variables.

## Usage

### The pipeline_config.toml file

The `pipeline_config.toml` file should include the credentials for three databases, source, workspace, and destination. It's okay for, say, the workspace and destination database to be the same, but the config file still needs the credentials filled in.


```toml
[source_db]
host = "<host name or ip address>"
dbname = "<db name>"
user = "<username>"
password = "<your password>"

[workspace_db]
host = "<host name or ip address>"
dbname = "<db name>"
user = "<username>"
password = "<your password>"

[destination_db]
host = "<host name or ip address>"
dbname = "<db name>"
user = "<username>"
password = "<your password>"
```

#### What each database is used for

##### Source

This is where the raw, non-aggregated data lives.

##### Workspace

This database stores the table metadata--the 'recipes' used to aggregate values
from the raw table to create the final tables.


##### Destination

This is the database where the final tables will end up.


### Simplest case

In the simplest case, if you just provide pipeline.py with a tablename, the aggregator will use options set on the table in the workspace db to fill in, otherwise will provide reasonable defaults.

```shell
>python pipeline.py b01982
```

### Optional arguments
#### Edition
#### Destination schema
#### Rebuild metadata
#### Hollow tables
