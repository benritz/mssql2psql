"use strict";

function writeTable(table, columns) {
	let f = (resolve, reject) => {
		// get columns def
        let columnDefs = "", columnsClause = "", identity = false;

		for (let column of columns) {
			if (columnDefs)
				columnDefs += ",\n";

            const columnName = `"${column.column_name}"`;

            columnDefs += columnName + " " + column.type;

            column.unicode = column.type === "nchar" || column.type === "nvarchar" || column.type === "ntext";

            if (!(column.type === "bit" ||
                column.type === "tinyint" ||
                column.type === "int" ||
                column.type === "smallint" ||
                column.type === "bigint" ||
                column.type === "float" ||
                column.type === "real" ||
                column.type === "money" ||
                column.type === "smallmoney" ||
                column.type === "date" ||
                column.type === "datetime" ||
                column.type === "smalldatetime" ||
				column.type === "text" ||
				column.type === "ntext" ||
				column.type === "image" ||
                column.type === "rowversion")) {

                let maxLen = column.max_length;

                columnDefs += "(";

                if (column.type === "char" ||
                    column.type === "nchar" ||
					column.type === "varchar" ||
					column.type === "nvarchar" ||
                    column.type === "binary" ||
					column.type === "varbinary") {
                    if (maxLen === -1) {
                        columnDefs += "max";
                    } else if (column.type === "nvarchar") {
                        columnDefs += maxLen / 2;
                    } else {
                        columnDefs += maxLen;
                    }
				} else {
                    if (column.type === "datetime2" ||
                        column.type === "time") {
                        columnDefs += column.scale;
                    } else if (column.precision) {
                        columnDefs += column.precision;
                        if (column.scale) {
                            columnDefs += "," + column.scale;
                        }
					}
				}

                columnDefs += ")";

            }

			if (!column.is_nullable) {
				columnDefs += " not";
			}

			columnDefs += " null";

            if (column.is_identity) {
                columnDefs += " identity";
                identity = true;
            }

            // columns clause
            if (columnsClause)
                columnsClause += ", ";

            columnsClause += columnName;
		}		

		// create streaming recordset
		// write table def when the recordset is opened and then write insert for each row
		// although this is done in a promise the ms sql tedious driver blocks other 
		// recordset streams until this one is finished so the output for a table is
		// sequential
        let request = new sql.Request();
		request.stream = true;

		request.query(`select * from ${table}`);

		request.on("recordset", () => {
			// write table def
            out.write(`/* -- ${table} -- */\nif object_id('${table}', 'U') is not null\n\tdrop table "${table}"\nGO\n\ncreate table "${table}"\n(\n${columnDefs}\n)\nGO\n\n`);
        });

        let n = 0;

		request.on('row', (row) => {
            if (n === 0 && identity)
                out.write(`set identity_insert "${table}" on\n\n`);

            let values = "";

			Object.keys(row).forEach((field, n) => {
				const column = columns[n];

                let value = row[field];

				if (values) {
					values += ", ";
				}

				if (typeof value === "string") {
					if (column.unicode) {
                        values += "N";
					}

                    values += "'" + value.replace(/'/g, "''") + "'";
				} else if (typeof value === "boolean") {
					values += value ? "1" : "0";
				} else if (value instanceof Date) {
					values += "'" + value.toISOString() + "'";
				} else {
					values += value;
				}
			});

			const insertPrefix = `insert into "${table}" (${columnsClause}) values `;

			if (options.dataBatchSize === 1) {
				out.write(`${insertPrefix}(${values})\nGO\n\n`);
			} else {
				if (n % options.dataBatchSize === 0) {
					if (n !== 0) {
						out.write("\nGO\n\n");
					}
					out.write(`${insertPrefix}(${values})`);
				} else {
					out.write(`\n,(${values})`);
				}
			}

			++n;
		});

		request.on('error', (err) => {
			reject(err);
		});

		request.on('done', (/* affected */) => {

			if (n !== 0) {
                out.write("\n");

                if (identity)
                    out.write(`\nset identity_insert "${table}" off\n`);

                out.write("GO\n\n");
			}

			resolve();
		});
	};

	return new Promise((resolve, reject) => { f(resolve, reject); });
}

function writeTables() {
    return sql.query`select t.name as table_name, c.column_id, c.name as column_name, c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, ty.name as type from sys.tables t, sys.columns c, sys.types ty where t.object_id = c.object_id and c.user_type_id = ty.user_type_id order by t.name asc, t.object_id asc, c.column_id asc`
		.then((result) => {
			out.write("/* --------------------- TABLES --------------------- */\n\n");

            const rows = result.recordset;

			// write table DDL and data
			let tables = [];

			if (rows.length) {
				let table, columns;

				for (let row of rows) {
					if (row.column_id === 1) {
						if (table) {
							tables.push({ table: table, columns: columns });
						}

						table = row.table_name;
						columns = [];
					}

					let column = row;
					delete column.table_name;
					columns.push(column);
				}

				tables.push({ table: table, columns: columns });
			}

            return tables.reduce((p, tableDef) => p.then(() => writeTable(tableDef.table, tableDef.columns) ), Promise.resolve());
		})
		.catch((e) => {
			console.error(e);
            console.trace();
		});
}

/**
 * Write foreign key defs.
 */
function writeForeignKeyes() {
	out.write("/* --------------------- FOREIGN KEYS --------------------- */\n\n");

	return sql.query`select 
fk.name as key_name, t.name as parent_table, c.name as parent_column, rt.name as referenced_table, rc.name as referenced_column, fkc.constraint_column_id as constraint_column_id
from 
sys.tables t, sys.tables rt, sys.columns c, sys.columns rc, sys.foreign_keys fk, sys.foreign_key_columns fkc 
where 
fk.object_id = fkc.constraint_object_id and 
t.object_id = fk.parent_object_id and fkc.parent_column_id = c.column_id and c.object_id = t.object_id and
rt.object_id = fk.referenced_object_id and fkc.referenced_column_id = rc.column_id and rc.object_id = rt.object_id
order by 
fk.object_id asc, fkc.constraint_column_id asc`
		.then((result) => {
			const rows = result.recordset;

			if (rows.length) {
				let key, parentColumns, parentTable, referencedColumns, referencedTable;

				let f = () => { out.write(`alter table "${parentTable}" add constraint ${key} foreign key (${parentColumns}) references "${referencedTable}" (${referencedColumns});\n`); };

				for (let row of rows) {
					if (row.constraint_column_id === 1) {
						if (parentTable) {
							f();

							if (parentTable !== row.parent_table) {
								out.write("\n");
							}
						}

						key = row.key_name;
						parentTable = row.parent_table;
						referencedTable = row.referenced_table;
						parentColumns = referencedColumns = "";
					} else {
						parentColumns += ", ";
						referencedColumns += ", ";
					}

					parentColumns += `"${row.parent_column}"`;
					referencedColumns += `"${row.referenced_column}"`;
				}

				f();

                out.write("GO\n\n");
			}
		});
}

/**
 * Write primary key, unique key and index defs.
 */
function writeIndexes() {
	out.write("/* --------------------- PRIMARY KEYS, UNIQUE CONSTRAINTS AND INDEXES --------------------- */\n\n");

	return sql.query`select 
t.name as table_name, c.name as column_name, i.name as index_name, ic.index_column_id, i.is_primary_key, i.is_unique_constraint, i.is_unique 
from 
sys.indexes i, sys.index_columns ic, sys.tables t, sys.columns c 
where 
i.object_id = t.object_id and ic.object_id = t.object_id and ic.index_id = i.index_id and t.object_id = c.object_id and 
ic.column_id = c.column_id 
order by t.name asc, t.object_id asc, i.index_id asc, ic.index_column_id asc`
		.then((result) => {
			const rows = result.recordset;

			if (rows.length) {
				let indexName, table, columns, isPrimary, isUniqueConst, isUnique;

				let f = () => {
					if (isPrimary) {
						out.write(`alter table ${table} add constraint "${indexName}" primary key (${columns});\n`);
					} else if (isUniqueConst) {
						out.write(`alter table ${table} add constraint "${indexName}" unique (${columns});\n`);
					} else if (isUnique) {
						out.write(`create unique index "${indexName}" on "${table}" (${columns});\n`);
					} else {
						out.write(`create index "${indexName}" on "${table}" (${columns});\n`);
					}
				};

				for (let row of rows) {
					if (row.index_column_id === 1) {
						if (table) {
							f();

							if (table !== row.table_name) {
								out.write("\n");
							}
						}

						table = row.table_name;
						indexName = row.index_name;
						isPrimary = row.is_primary_key;
						isUniqueConst = row.is_unique_constraint;
						isUnique = row.is_unique;
						columns = "";
					} else {
						columns += ", ";
					}

					columns += `"${row.column_name}"`;
				}

				f();

                out.write("GO\n\n");
			}
		});
}

/**
 * Write default defs.
 */
function writeDefaults() {
	out.write("/* --------------------- DEFAULTS --------------------- */\n\n");

	return sql.query`select
t.name as table_name, c.name as column_name, ty.name as type, d.name as default_name, d.definition 
from 
sys.tables t, sys.columns c, sys.types ty, sys.default_constraints d 
where 
t.object_id = c.object_id and c.user_type_id = ty.user_type_id and t.object_id = d.parent_object_id and c.column_id = d.parent_column_id
order by t.name asc, t.object_id asc, c.column_id asc`
		.then((result) => {
			const rows = result.recordset;

			if (rows.length) {
				let table;
				for (let row of rows) {
					if (table && table !== row.table_name) {
						out.write("\n");
					}

					table = row.table_name;

                    let definition = row.definition;

                    definition = definition.replace(/^\(\((\d+)\)\)$/, "$1");

                    out.write(`alter table "${table}" add constraint "${row.default_name}" default ${definition} for "${row.column_name}";\n`);
				}

				out.write("GO\n\n");
			}
		});
}

function writeCompiledObject(type) {
    return sql.query`select c.text, c.colid from sysobjects o, syscomments c where c.id = o.id and o.type = ${type} order by o.name, c.colid asc`
        .then((result) => {
            const rows = result.recordset;

            if (rows.length) {
            	let n = 0;

                for (let row of rows) {
                	if (row.colid === 1) {
                		if (n > 0) {
                            out.write(";\nGO\n\n");
						}

                        ++n;
					}

                    out.write(row.text.trim());
                }

                out.write(";\nGO\n\n");
            }
        });
}

/**
 * Write functions defs.
 */
function writeFunctions() {
    out.write("/* --------------------- FUNCTIONS --------------------- */\n\n");

    return writeCompiledObject("FN");
}

/**
 * Write procedures defs.
 */
function writeProcedures() {
    out.write("/* --------------------- PROCEDURES --------------------- */\n\n");

    return writeCompiledObject("P");
}

/**
 * Write view defs.
 */
function writeViews() {
    out.write("/* --------------------- VIEWS --------------------- */\n\n");

    return writeCompiledObject("V");
}

/**
 * Write trigger defs.
 */
function writeTriggers() {
    out.write("/* --------------------- TRIGGERS --------------------- */\n\n");

    return writeCompiledObject("TR");
}

const minimist = require('minimist');

const options = minimist(process.argv.slice(2), {
	default: { "dataBatchSize": 100 } });
const args = options._;
delete options._;

if (args.length === 0) {
	console.error("Missing the connection URL.");
	process.exit(1);
}

const url = args[0];

let out;

if (args.length === 1) {
	out = process.stdout;
} else {
	const fs = require('fs');

	out = fs.createWriteStream(args[1]);

	// UTF-8 DOM for Windows
    out.write("\ufeff");
}

const ConnectionString = require('mssql/lib/connectionstring');

const config = ConnectionString.resolve(url);

if (typeof config.requestTimeout === "undefined") {
    config.requestTimeout = 60 * 60 * 1000;
}

if (typeof config.connectionTimeout === "undefined") {
	config.connectionTimeout = 60 * 60 * 1000;
}

const sql = require('mssql');

sql.connect(config)
	.then(writeTables)
	.then(writeIndexes)
	.then(writeForeignKeyes)
    .then(writeFunctions)
	.then(writeDefaults)
    .then(writeProcedures)
	.then(writeViews)
    .then(writeTriggers)
	.then(() => {
		if (out !== process.stdout) {
			out.end((err) => {
				process.exit(0);
			});
		}  else {
			process.exit(0);
		}
	})
	.catch((e) => {
     	console.error(e);
        console.trace();
     	process.exit(1);
});
