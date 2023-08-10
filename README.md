# logs-traces-demo

Viz demo of logs and traces using Streamlit

## Setup

```
conda env create --file environment.yml
conda activate logs-demo
```

Or: 

```
python -m venv venv
source venv/scripts/activate
```

Set the following environment variables with your Snowflake account information:

```bash
# Linux/MacOS
export SNOWSQL_ACCOUNT=<replace with your account identifer>
export SNOWSQL_USER=<replace with your username>
export SNOWSQL_ROLE=<replace with your role>
export SNOWSQL_PWD=<replace with your password>
export SNOWSQL_DATABASE=<replace with your database>
export SNOWSQL_SCHEMA=<replace with your schema>
export SNOWSQL_WAREHOUSE=<replace with your warehouse>
```

```powershell
# Windows/PowerShell
$env:SNOWSQL_ACCOUNT = "<replace with your account identifer>"
$env:SNOWSQL_USER = "<replace with your username>"
$env:SNOWSQL_ROLE = "<replace with your role>"
$env:SNOWSQL_PWD = "<replace with your password>"
$env:SNOWSQL_DATABASE = "<replace with your database>"
$env:SNOWSQL_SCHEMA = "<replace with your schema>"
$env:SNOWSQL_WAREHOUSE = "<replace with your warehouse>"
```