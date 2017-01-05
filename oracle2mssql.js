"use strict";

function writeTableDDL(table, columns) {
    return new Promise((resolve, reject) => {
        // get columns defs
        let columnDefs = "";

        for (let column of columns) {
            if (columnDefs)
                columnDefs += ",\n";

            columnDefs += column.column_name + " ";

            if (column.type === "number") {
                if (column.scale === 0) {
                    switch (column.precision) {
                        case 20:
                            columnDefs += "bigint";
                            break;
                        case 10:
                        case null:    // default number precision, assume we wanted integer
                            columnDefs += "int";
                            break;
                        case 5:
                            columnDefs += "smallint";
                            break;
                        case 1:
                            columnDefs += "bit";
                            break;
                        default:
                            columnDefs += "decimal(" + column.precision + ")";
                            break;
                    }
                } else if (column.precision === 19 && column.scale === 4) {
                    columnDefs += "currency";
                } else {
                    columnDefs += "decimal(" + column.precision + ", " + column.scale + ")";
                }
            } else if (column.type === "varchar" || column.type === "varchar2") {
                columnDefs += "varchar(" + (column.max_length) + ")";
            } else if (column.type === "nvarchar" || column.type === "nvarchar2") {
                columnDefs += "nvarchar(" + (column.max_length) + ")";
            } else if (column.type === "clob") {
                columnDefs += "varchar(max)";
            } else if (column.type === "nclob") {
                columnDefs += "nvarchar(max)";
            } else if (column.type === "date" || column.type === "timestamp(6)") {
                columnDefs += "datetime";
            } else if (column.type === "long raw") {
                columnDefs += "image";
            } else {
                columnDefs += column.type;
            }

            if (!column.is_nullable) {
                columnDefs += " not";
            }

            columnDefs += " null";

            if (column.is_identity) {
                columnDefs += " identity";
            }
        }

        // write table def
        out.write(`/* -- ${table} -- */\nif object_id('${table}', 'U') is not null\n\tdrop table ${table}\nGO\n\ncreate table ${table}\n(\n${columnDefs}\n)\nGO\n\n`, "utf8", (err) => {
            if (err)
                reject(err);
            else
                resolve();
        });
    });
}

function writeTableDisableConstraints(table) {
    return new Promise((resolve, reject) => {
        out.write(`/* -- ${table} -- */\nif object_id('${table}', 'U') is not null\nbegin\n\talter table ${table} nocheck constraint all\n\talter table ${table} disable trigger all\nend\nGO\n\n`, "utf8", (err) => {
            if (err)
                reject(err);
            else
                resolve();
        });
    });
}

function writeTableEnableConstraints(table) {
    return new Promise((resolve, reject) => {
        out.write(`/* -- ${table} -- */\nalter table ${table} with check check constraint all\nGO\nalter table ${table} enable trigger all\nGO\n\n`, "utf8", (err) => {
            if (err)
                reject(err);
            else
                resolve();
        });
    });
}

function writeTableData(table, columns, truncate) {
    return new Promise((resolve, reject) => {
        // get select clause
        let selectClause = "", columnsClause = "", identity = false;

        for (let column of columns) {
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
            // create streaming result set and write out inserts
            connection.execute(`select ${selectClause} from ${table}`, {}, { resultSet: true , outFormat: oracledb.OBJECT }).then((result) => {
                let stream = result.resultSet.toQueryStream();

                let n = 0, onData = 0, onEnd = false;

                const end = () => {
                    if (n !== 0) {
                        let data = `\nGO\n\n`;

                        if (identity)
                            data += `set identity_insert ${table} off\n\n`;

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

                                readValue = toStr(value).then((value) => { a.push(value); }).catch(() => { reject(err) });
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
                                values += "'" + value.replace("'", "''") + "'";
                            } else if (value instanceof Date) {
                                values += "'" + value.toISOString() + "'";
                            } else {
                                values += value;
                            }
                        }

                        let data = "";

                        if (n === 0 && identity) {
                            data += `set identity_insert ${table} on\n\n`;
                        }

                        if (options.dataBatchSize === 1) {
                            data += `insert into ${table} (${columnsClause}) values (${values})\nGO\n`;
                        } else {
                            if (n % options.dataBatchSize === 0) {
                                if (n !== 0)
                                    data += "\nGO\n\n";
                                data += `insert into ${table} (${columnsClause}) values (${values})`;
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
            });
        };

        let data = `/* -- ${table} -- */\n`;

        if (truncate) {
            data += `delete from ${table}\nGO\n\n`;
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

function writeTables() {
    return connection.execute(`select 
upper(c.TABLE_NAME) as "table_name", c.COLUMN_ID as "column_id", upper(c.COLUMN_NAME) as "column_name", c.DATA_LENGTH as "max_length", c.DATA_PRECISION as "precision", c.DATA_SCALE as "scale", case c.NULLABLE when 'Y' then 1 else 0 end as "is_nullable", case when trc.COLUMN_NAME is null then 0 else 1 end as "is_identity", lower(c.DATA_TYPE) as "type" 
from 
(USER_TAB_COLUMNS c left join USER_TRIGGERS tr on tr.TABLE_NAME = c.TABLE_NAME and tr.TRIGGER_TYPE = 'BEFORE EACH ROW' and tr.TRIGGERING_EVENT = 'INSERT') left join USER_TRIGGER_COLS trc on tr.TRIGGER_NAME = trc.TRIGGER_NAME and tr.TABLE_NAME = trc.TABLE_NAME and trc.COLUMN_NAME = c.COLUMN_NAME and trc.COLUMN_USAGE = 'NEW IN OUT'
order by c.TABLE_NAME asc, c.COLUMN_ID asc`, {}, { outFormat: oracledb.OBJECT, maxRows: 100000 }).then((result) => {
        // read table metadata
        let tables = {};

        if (result.rows.length) {
            let table, columns;

            for (let row of result.rows) {
                if (row.column_id === 1) {
                    if (table) {
                        tables[table] = columns;
                    }

                    table = row.table_name;
                    columns = [];
                }

                let column = row;
                delete column.table_name;
                columns.push(column);
            }

            tables[table] = columns;
        }

        let p = Promise.resolve();

        if (options.createTable === true) {
            // creating all tables
            for (let table of Object.keys(tables)) {
                let columns = tables[table];

                p = p.then(writeTableDDL.bind(this, table, columns));
                p = p.then(writeTableData.bind(this, table, columns, false /* don't truncate data */));
            }
        } else {
            for (let table of Object.keys(tables)) {
                p = p.then(writeTableDisableConstraints.bind(this, table));
            }

            if (Array.isArray(options.createTable)) {
                for (let table of options.createTable) {
                    let columns = tables[table];
                    if (columns) {
                        p = p.then(writeTableDDL.bind(this, table, columns));
                    }
                }
            }

            for (let table of Object.keys(tables)) {
                p = p.then(writeTableData.bind(this, table, tables[table], true /* truncate data */));
            }

            for (let table of Object.keys(tables)) {
                p = p.then(writeTableEnableConstraints.bind(this, table));
            }
        }

        return p;
	});
}

function getForeignKeys(table, referenced = false) {
    let sql = `select
    p.constraint_name as "key_name", p.table_name as "parent_table", pc.column_name as "parent_column", r.table_name as "referenced_table", rc.column_name as "referenced_column", rc.position as "constraint_column_id"
    from
    user_constraints p  join user_cons_columns pc on p.owner = pc.owner and p.constraint_name = pc.constraint_name join user_constraints r on p.r_owner = r.owner and p.r_constraint_name = r.constraint_name join user_cons_columns rc on r.owner = rc.owner and r.constraint_name = rc.constraint_name
    where
    p.constraint_type = 'R' and `;

    if (referenced)
        sql += `r`;
    else
        sql += `p`;

    sql += `.table_name = '${table}' order by p.table_name asc, pc.position asc`;

    return connection.execute(sql, {}, { outFormat: oracledb.OBJECT, maxRows: 100000 }).then((result) => {
        let keys = [];

        if (result.rows.length) {
            let keyData;

            for (let row of result.rows) {
                if (row.constraint_column_id === 1) {
                    if (keyData) {
                        keys.push(keyData);
                    }

                    keyData = { key: row.key_name, parentTable: row.parent_table, parentColumns: [], referencedTable: row.referenced_table, referencedColumns: [] };
                }

                keyData.parentColumns.push(row.parent_column);
                keyData.referencedColumns.push(row.referenced_column);
            }

            keys.push(keyData);
        }

        return keys;
    });
}

// read options from command line
let minimist = require('minimist');

let options = minimist(process.argv.slice(2), {
    boolean: ["forceCaseInsensitive"],
    default: { "dataBatchSize": 100, "forceCaseInsensitive": true, "createTable": false } });
let args = options._;
delete options._;

if (typeof options.createTable === "string")
    options.createTable = [options.createTable.toUpperCase()];
else if (Array.isArray(options.createTable))
    options.createTable = options.createTable.filter(function(e) { return typeof e === "string"; }).map(function(e) { return e.toUpperCase(); });

if (args.length === 0) {
    console.error("Missing the connection URL.");
    process.exit(1);
}

const url = require('url');

let connectUrl = url.parse(args[0]);

if (connectUrl.protocol !== "oracle:" || connectUrl.auth === null || connectUrl.hostname === null) {
    console.error("Invalid connection URL");
    process.exit(1);
}

let connectAuth = connectUrl.auth.split(":");

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
}

// connect and dump out statements
let connection = null;

let oracledb = require('oracledb');

oracledb.getConnection({ user: connectAuth[0], password: connectAuth[1], connectString: connectUrl.hostname + connectUrl.path })
	.then((c) => {
        connection = c;

        return getForeignKeys("IBT_ITEM", true).then(function(keys) { console.log(keys); }).catch(function(err) { console.error(err); });
		//writeTables().then(() => { c.close().then(() => { connection = null; }); });
	})
	.then(() => {
        process.exit(0);
	})
	.catch((err) => {
		console.error(err);
	});
