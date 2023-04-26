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


### Example

This will create data profile for MySQL's [sakila dataset](https://dev.mysql.com/doc/sakila/en/).

#### Download and load sakila dataset

```bash
curl https://downloads.mysql.com/docs/sakila-db.tar.gz -o /tmp/sakila-db.tar.gz
tar zxf /tmp/sakila-db.tar.gz -C /tmp/
# create sakila schema, along with any UDF and UDP
cat /tmp/sakila-db/sakila-schema.sql | mysql -u root -p
cat /tmp/sakila-db/sakila-data.sql | mysql -u root -p
```

#### Setup .env

```bash
cp .env.sample .env
# edit .env and specify username, password, host, and port in there
```

#### Run dbt-profiler


The following will profile every column in every table in the `sakila` schema:
```bash
 ./dbtw run --target dev --vars '{"table_schema":"sakila","profile_materialization":"table"}'
```

#### Results

Finally, simply query the `dbt_profiler.profile` table:

```SQL
SELECT * FROM dbt_profiler.profile
```

Extract of results:
![sakila results](static/dbt_profiled_sakila.png)

# References

https://github.com/hpcc-systems/DataPatterns
https://stackoverflow.com/questions/74898764/iterate-over-all-rows-and-columns-in-dbt-jinja
https://serge-g.medium.com/dynamic-sql-pivots-with-dbt-dea16d7b9b63
