# Setup

Run `setup.sh` or follow these steps:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip setuptools wheel
python3 -m pip install -r requirements.txt
```

## Usage

The dbt wrapper `dbtw` was created to load environment variable when running dbt.
Instead of doing:

```bash
dbt --version
dbt debug
```

Use `dbtw` instead:

```bash
dbtw --version
dbtw debug
```

Or load/export environment variables yourself and use `dbt` as usual.

Specify the table schema and table name either in profiles.yml or from command line, like so:

```bash
./dbtw run --vars '{"table_schema":"myschema","table_name":"mytable"}'
```


### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


https://serge-g.medium.com/dynamic-sql-pivots-with-dbt-dea16d7b9b63
