"use strict";

let values = require('object.values');

if (!Object.values) {
    values.shim();
}

function writeDropReferencedForeignKeyDDL(table) {
    if (table.referencedForeignKeys.length === 0)
        return;

    return new Promise((resolve, reject) => {
        let ddl = `/* -- ${table.name} -- */\n\n`;

        for (let key of table.referencedForeignKeys) {
            ddl += `if object_id('${key.key}', 'F') is not null\nbegin\n\talter table "${key.parentTable}" drop constraint "${key.key}"\nend\nGO\n\n`;
        }

        // write table def
        out.write(ddl, "utf8", (err) => {
            if (err)
                reject(err);
            else
                resolve();
        });
    });
}

function writeForeignKeyDDL(table) {
    if (table.foreignKeys.length === 0 || table.referencedForeignKeys.length === 0)
        return;

    return new Promise((resolve, reject) => {
        let ddl = `/* -- ${table.name} -- */\n\n`;

        let keyDDL = (key) => {
            let colsToStr = (columns) => { return columns.map(function(column) { return "\"" + column + "\""; }).join(", ") };

            return `alter table "${key.parentTable}" add constraint "${key.key}" foreign key (${colsToStr(key.parentColumns)}) references "${key.referencedTable}" (${colsToStr(key.referencedColumns)})\nGO\n\n`;
        };

        for (let key of table.foreignKeys) {
            ddl += keyDDL(key);
        }

        for (let key of table.referencedForeignKeys) {
            ddl += keyDDL(key);
        }

        // write table def
        out.write(ddl, "utf8", (err) => {
            if (err)
                reject(err);
            else
                resolve();
        });
    });
}

function writeTableDDL(table) {
    return new Promise((resolve, reject) => {
        // get columns defs
        let defs = "";

        for (let column of table.columns) {
            if (defs)
                defs += ",\n";

            defs += "\"" + column.column_name + "\" ";

            if (column.type === "number") {
                if (column.scale === 0) {
                    switch (column.precision) {
                        case 20:
                            defs += "bigint";
                            break;
                        case 10:
                        case null:    // default number precision, assume we wanted integer
                            defs += "int";
                            break;
                        case 5:
                            defs += "smallint";
                            break;
                        case 1:
                            defs += "bit";
                            break;
                        default:
                            defs += "decimal(" + column.precision + ")";
                            break;
                    }
                } else if (column.precision === 19 && column.scale === 4) {
                    defs += "currency";
                } else {
                    defs += "decimal(" + column.precision + ", " + column.scale + ")";
                }
            } else if (column.type === "varchar" || column.type === "varchar2") {
                defs += "varchar(" + (column.max_length) + ")";
            } else if (column.type === "nvarchar" || column.type === "nvarchar2") {
                defs += "nvarchar(" + (column.max_length) + ")";
            } else if (column.type === "clob") {
                defs += "varchar(max)";
            } else if (column.type === "nclob") {
                defs += "nvarchar(max)";
            } else if (column.type === "date" || column.type === "timestamp(6)") {
                defs += "datetime";
            } else if (column.type === "long raw") {
                defs += "image";
            } else {
                defs += column.type;
            }

            if (!column.is_nullable) {
                defs += " not";
            }

            defs += " null";

            if (column.is_identity) {
                defs += " identity";
            }
        }

        if (table.primaryKey) {
            let pkCols = table.primaryKey.columns.map(function(column) { return "\"" + column + "\""; }).join(", ");

            defs += `,\n\nconstraint "${table.primaryKey.key}" primary key (${pkCols})`;
        }

        let ddl = `/* -- ${table.name} -- */\nif object_id('${table.name}', 'U') is not null\n\tdrop table "${table.name}"\nGO\n\ncreate table "${table.name}"\n(\n${defs}\n)\nGO\n\n`;

        // write table def
        out.write(ddl, "utf8", (err) => {
            if (err)
                reject(err);
            else
                resolve();
        });
    });
}

function writeTableDisableConstraints(table) {
    return new Promise((resolve, reject) => {
        out.write(`/* -- ${table.name} -- */\nif object_id('${table.name}', 'U') is not null\nbegin\n\talter table ${table.name} nocheck constraint all\n\talter table ${table.name} disable trigger all\nend\nGO\n\n`, "utf8", (err) => {
            if (err)
                reject(err);
            else
                resolve();
        });
    });
}

function writeTableEnableConstraints(table) {
    return new Promise((resolve, reject) => {
        out.write(`/* -- ${table.name} -- */\nalter table ${table.name} with check check constraint all\nGO\nalter table ${table.name} enable trigger all\nGO\n\n`, "utf8", (err) => {
            if (err)
                reject(err);
            else
                resolve();
        });
    });
}

function writeTableData(table, truncate) {
    return new Promise((resolve, reject) => {
        // get select clause
        let selectClause = "", columnsClause = "", identity = false;

        for (let column of table.columns) {
            if (column.type === "long raw")     // long raw columns cause oracledb to crash
                continue;

            // select clause
            // oracledb doesn't support streaming NCLOB columns so convert them to CLOB
            if (selectClause)
                selectClause += ", ";

            if (column.type === "nclob")
                selectClause += "to_clob(\"" + column.column_name + "\") as \"" + column.column_name + "\"";
            else
                selectClause += "\"" + column.column_name + "\"";

            // columns clause
            if (columnsClause)
                columnsClause += ", ";

            columnsClause += "\"" + column.column_name + "\"";

            // check for identity column
            if (column.is_identity)
                identity = true;
        }

        let writeData = () => {
            const minDate = new Date(1753, 0, 1);
            const minDateTime = minDate.getTime();

            // create streaming result set and write out inserts
            connection.execute(`select ${selectClause} from "${table.name}"`, {}, { resultSet: true , outFormat: oracledb.OBJECT }).then((result) => {
                let stream = result.resultSet.toQueryStream();

                let n = 0, onData = 0, onEnd = false;

                const end = () => {
                    if (n !== 0) {
                        let data = `\nGO\n\n`;

                        if (identity)
                            data += `set identity_insert ${table.name} off\n\n`;

                        out.write(data, "utf8", (err) => { if (err) reject(err); else resolve(); });
                    } else {
                        resolve();
                    }
                };

                stream.on("data", (row) => {
                    ++onData;

                    // get values (uses promises as LOB columns need to be streamed)
                    let a = [], p = Promise.resolve(), readValue;

                    for (let field of Object.keys(row)) {
                        let value = row[field];

                        if (oracledb.Lob.prototype.isPrototypeOf(value)) {
                            if (value.type === oracledb.CLOB) {
                                let toStr = (str) => {
                                    return new Promise((resolve, reject) => {
                                        let s = "";
                                        str.setEncoding("utf8");
                                        str.on('data', (chunk) => { s += chunk; });
                                        str.on('end', () => { resolve(s); });
                                        str.on('error', (err) => { reject(err); });
                                    });
                                };

                                readValue = () => { return toStr(value).then((value) => { a.push(value); }) };
                            } else {
                                // blobs are not supported yet (need to convert them to hex string?)
                                readValue = () => { a.push(null); };
                            }
                        } else {
                            readValue = () => { a.push(value); };
                        }

                        p = p.then(readValue);
                    }

                    p.then(() => {
                        // write insert
                        let values = "";

                        for (let value of a) {
                            if (values) {
                                values += ", ";
                            }

                            if (typeof value === "string") {
                                values += "'" + value.replace(/'/g, "''") + "'";
                            } else if (value instanceof Date) {
                                if (value.getTime() < minDateTime)
                                    value = minDate;

                                values += "'" + value.toISOString() + "'";
                            } else {
                                values += value;
                            }
                        }

                        let data = "";

                        if (n === 0 && identity) {
                            data += `set identity_insert ${table.name} on\n\n`;
                        }

                        if (options.dataBatchSize === 1) {
                            data += `insert into ${table.name} (${columnsClause}) values (${values})\nGO\n`;
                        } else {
                            if (n % options.dataBatchSize === 0) {
                                if (n !== 0)
                                    data += "\nGO\n\n";
                                data += `insert into ${table.name} (${columnsClause}) values (${values})`;
                            } else {
                                data += `\n,(${values})`;
                            }
                        }

                        out.write(data, "utf8", (err) => {
                            if (err) {
                                reject(err);
                                return
                            }

                            if (onEnd && onData === 1)
                                end();

                            --onData;
                        });

                        ++n;
                    });
                });

                stream.on("error", (err) => {
                    console.error(err);
                    reject(err);
                });

                stream.on("end", () => {
                    onEnd = true;

                    if (onData === 0)
                        end();
                });
            }).catch((err) => { console.error(err) });
        };

        let data = `/* -- ${table.name} -- */\n`;

        if (truncate) {
            data += `delete from ${table.name}\nGO\n\n`;
        }

        out.write(data, "utf8", (err) => {
            if (err) {
                reject(err);
                return;
            }

            writeData();
        });
    });
}

function getTableMetadata() {
    return connection.execute(`select 
upper(c.TABLE_NAME) as "table_name", c.COLUMN_ID as "column_id", upper(c.COLUMN_NAME) as "column_name", c.DATA_LENGTH as "max_length", c.DATA_PRECISION as "precision", c.DATA_SCALE as "scale", case c.NULLABLE when 'Y' then 1 else 0 end as "is_nullable", case when trc.COLUMN_NAME is null then 0 else 1 end as "is_identity", lower(c.DATA_TYPE) as "type" 
from 
(USER_TAB_COLUMNS c left join USER_TRIGGERS tr on tr.TABLE_NAME = c.TABLE_NAME and tr.TRIGGER_TYPE = 'BEFORE EACH ROW' and tr.TRIGGERING_EVENT = 'INSERT') left join USER_TRIGGER_COLS trc on tr.TRIGGER_NAME = trc.TRIGGER_NAME and tr.TABLE_NAME = trc.TABLE_NAME and trc.COLUMN_NAME = c.COLUMN_NAME and trc.COLUMN_USAGE = 'NEW IN OUT'
order by c.TABLE_NAME asc, c.COLUMN_ID asc`, {}, { outFormat: oracledb.OBJECT, maxRows: 100000 }).then((result) => {
        // read table metadata
        let tables = {};

        if (result.rows.length) {
            let tableData;

            const addTable = (tableData) => {
                if (options.ignoreTable.indexOf(tableData.name) === -1)
                    tables[tableData.name] = tableData;
            };

            for (let row of result.rows) {
                if (row.column_id === 1) {
                    if (tableData) {
                        addTable(tableData);
                    }

                    tableData = { name: row.table_name, columns: [], primaryKey: null, foreignKeys: [], referencedForeignKeys: [] };
                }

                let column = row;
                delete column.table_name;
                tableData.columns.push(column);
            }

            addTable(tableData);
        }

        return tables;
    });
}

function getPrimaryKeyMetadata(tables) {
    let sql = `select
c.constraint_name as "key_name", c.table_name as "table", cc.column_name as "column", cc.position as "constraint_column_id"
from
user_constraints c join user_cons_columns cc on c.owner = cc.owner and c.constraint_name = cc.constraint_name
where
c.constraint_type = 'P' order by c.table_name asc, cc.position asc`;

    return connection.execute(sql, {}, { outFormat: oracledb.OBJECT, maxRows: 100000 }).then((result) => {
        if (result.rows.length) {
            let keyData;

            let addKey = (keyData) => {
                let table = tables[keyData.table];
                if (table) {
                    table.primaryKey = keyData;
                }
            };

            for (let row of result.rows) {
                if (row.constraint_column_id === 1) {
                    if (keyData) {
                        addKey(keyData);
                    }

                    keyData = { key: row.key_name, table: row.table, columns: [] };
                }

                keyData.columns.push(row.column);
            }

            addKey(keyData);
        }

        return tables;
    });
}

function getForeignKeyMetadata(tables) {
    let sql = `select
p.constraint_name as "key_name", p.table_name as "parent_table", pc.column_name as "parent_column", r.table_name as "referenced_table", rc.column_name as "referenced_column", rc.position as "constraint_column_id"
from
user_constraints p  join user_cons_columns pc on p.owner = pc.owner and p.constraint_name = pc.constraint_name join user_constraints r on p.r_owner = r.owner and p.r_constraint_name = r.constraint_name join user_cons_columns rc on r.owner = rc.owner and r.constraint_name = rc.constraint_name
where
p.constraint_type = 'R' order by p.table_name asc, pc.position asc`;

    return connection.execute(sql, {}, { outFormat: oracledb.OBJECT, maxRows: 100000 }).then((result) => {
        if (result.rows.length) {
            let keyData;

            let addKeys = (keyData) => {
                let parent = tables[keyData.parentTable];
                if (parent) {
                    parent.foreignKeys.push(keyData);
                }

                let referenced = tables[keyData.referencedTable];
                if (referenced) {
                    referenced.referencedForeignKeys.push(keyData);
                }
            };

            for (let row of result.rows) {
                if (row.constraint_column_id === 1) {
                    if (keyData) {
                        addKeys(keyData);
                    }

                    keyData = { key: row.key_name, parentTable: row.parent_table, parentColumns: [], referencedTable: row.referenced_table, referencedColumns: [] };
                }

                keyData.parentColumns.push(row.parent_column);
                keyData.referencedColumns.push(row.referenced_column);
            }

            addKeys(keyData);
        }

        return tables;
    });
}

function getTables() {
    return getTableMetadata()
        .then(getPrimaryKeyMetadata)
        .then(getForeignKeyMetadata);
}

function writeTables(tables) {
    let p = Promise.resolve();

    if (options.createTable === true) {
        // creating all tables + inserting data
        for (let table of Object.values(tables)) {
            p = p.then(() => { return writeTableDDL(table) });
        }

        for (let table of Object.values(tables)) {
            p = p.then(() => { return writeTableData(table, false /* don't truncate data */) });
        }

        for (let table of Object.values(tables)) {
            p = p.then(() => { return writeForeignKeyDDL(table) });
        }
    } else {
        // optionally creating some tables + inserting data
        if (Array.isArray(options.createTable)) {
            let createTables = options.createTable.map((tableName) => { return tables[tableName]; });

            for (let table of createTables) {
                p = p.then(() => { return writeDropReferencedForeignKeyDDL(table) });
            }

            for (let table of createTables) {
                p = p.then(() => { return writeTableDDL(table) });
            }
        }

        for (let table of Object.values(tables)) {
            p = p.then(() => { return writeTableDisableConstraints(table) });
        }

        for (let table of Object.values(tables)) {
            p = p.then(() => { return writeTableData(table, true /* truncate data */) });
        }

        if (Array.isArray(options.createTable)) {
            let createTables = options.createTable.map((tableName) => { return tables[tableName]; });

            for (let table of createTables) {
                p = p.then(() => { return writeForeignKeyDDL(table) });
            }
        }

        for (let table of Object.values(tables)) {
            p = p.then(() => { return writeTableEnableConstraints(table) });
        }
    }

    return p;
}

// read options from command line
const minimist = require('minimist');

const options = minimist(process.argv.slice(2), {
    boolean: ["forceCaseInsensitive"],
    default: { "dataBatchSize": 100, "forceCaseInsensitive": true, "createTable": false, "ignoreTable": [] } });
const args = options._;
delete options._;

if (typeof options.createTable === "string")
    options.createTable = [options.createTable.toUpperCase()];
else if (Array.isArray(options.createTable))
    options.createTable = options.createTable.filter(function(e) { return typeof e === "string"; }).map(function(e) { return e.toUpperCase(); });

if (typeof options.ignoreTable === "string")
    options.ignoreTable = [options.ignoreTable.toUpperCase()];
else if (Array.isArray(options.ignoreTable))
    options.ignoreTable = options.ignoreTable.filter(function(e) { return typeof e === "string"; }).map(function(e) { return e.toUpperCase(); });

if (args.length === 0) {
    console.error("Missing the connection URL.");
    process.exit(1);
}

const url = require('url');

const connectUrl = url.parse(args[0]);

if (connectUrl.protocol !== "oracle:" || connectUrl.auth === null || connectUrl.hostname === null) {
    console.error("Invalid connection URL");
    process.exit(1);
}

const connectAuth = connectUrl.auth.split(":");

if (connectAuth.length !== 2) {
    console.error("Invalid connection URL");
    process.exit(1);
}

// setup output (to file or stdout)
let out;

if (args.length === 1) {
    out = process.stdout;
} else {
    let fs = require('fs');

    out = fs.createWriteStream(args[1]);

    // UTF-8 DOM for Windows
    out.write("\ufeff");
}

// connect and dump out statements
let connection = null;

const oracledb = require('oracledb');

oracledb.getConnection({ user: connectAuth[0], password: connectAuth[1], connectString: connectUrl.hostname + connectUrl.path })
	.then((c) => { connection = c; })
    .then(getTables)
    .then(writeTables)
    .then(() => {
        return connection.close();
    })
	.then(() => {
        connection = null;
        process.exit(0);
	})
	.catch((err) => {
		console.error(err);
	});
