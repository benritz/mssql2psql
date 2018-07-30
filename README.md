# mssql2psql

Generates SQL schema and data scripts.

Original written to migrate a MS SQL database to a PostgreSQL database. Now includes Oracle to MS SQL and MS SQL to MS SQL.

## Example Usage

```
node mssql2psql.js mssql://user:pwd@host:port/database out.sql
```

```
node mssql2mssql.js mssql://user:pwd@host:port/database out.sql
```

```
export LD_LIBRARY_PATH=/opt/oracle/instantclient_12_1:$LD_LIBRARY_PATH

node oracle2mssql.js oracle://user:pwd@host:port/service out.sql
```