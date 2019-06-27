"use strict";

function writeTable(schema, table, columns) {
	// get columns def
	let columnDefs = "";

	for (let column of columns) {
		if (columnDefs)
			columnDefs += ",\n";

		columnDefs += "\"" + column.column_name + "\" ";

		const type = column.column_type;

		if (type === "integer" || type === "serial") {
			columnDefs += "int";
		} else if (type === "double precision") {
			columnDefs += "float";
		} else if (type === "character varying" || type === "text") {
			columnDefs += "nvarchar";

			let maxLen = column.max_length;
			if (maxLen === -1) {
				maxLen = "max";
			}

			columnDefs += "(" + (maxLen) + ")";
		} else if (type === "timestamp") {
			columnDefs += "datetime";
		} else if (type === "timestamp with time zone") {
			columnDefs += "datetimeoffset";
		} else if (type === "bytea") {
			columnDefs += "image";
		} else if (type === "boolean") {
			columnDefs += "bit";
		} else if (type === "inet" || type === "geometry") {
			columnDefs += "varchar(255)";
		} else {
			columnDefs += type;
		}

		if (!column.is_nullable) {
			columnDefs += " not";
		}
		columnDefs += " null";

		if (type === "serial") {
			columnDefs += " identity";
		}
	}

	out.write(`/* -- ${table} -- */\nif object_id('${table}') is not null\nbegin\n\tdrop table ${table};\nend;\n\n\ncreate table ${table}\n(\n${columnDefs}\n);\nGO\n\n`);

	const qs = new QueryStream(`select * from "${schema}"."${table}"`);

	let n = 0;

	const writeRow = through((row) => {
		let values = "";

		Object.keys(row).forEach(function(field) {
			let value = row[field];

			if (values) {
				values += ", ";
			}

			if (typeof value === "boolean") {
				values += value ? "1" : "0";
			} else if (typeof value === "string") {
				values += "'" + value.replace(/'/g, "''") + "'";
			} else if (value instanceof Date) {
				values += "'" + value.toISOString() + "'";
			} else {
				values += value;
			}
		});

		if (options.dataBatchSize === 1) {
			out.write(`insert into ${table} values (${values})`);
		} else {
			if (n % options.dataBatchSize === 0) {
				if (n !== 0) {
					out.write(";\nGO\n\n");
				}
				out.write(`insert into ${table} values (${values})`);
			} else {
				out.write(`\n,(${values})`);
			}
		}

		++n;
	});

	return db.stream(qs, s => { s.pipe(writeRow); }).then(() => { if (n > 0) { out.write(";\nGO\n\n"); } });
}

function writeTables() {
	let sql = 'select n.nspname as schema, c.relname as table_name, a.attnum as column_id, a.attname as column_name, atttypid::regtype as column_type, a.attlen as max_length, case a.attnotnull when true then 0 else 1 end as is_nullable from pg_class c, pg_namespace n, pg_attribute a where c.relkind = $1 and c.relnamespace = n.oid and n.nspname = $2 and a.attrelid = c.oid and a.attnum > 0 order by c.oid, a.attnum',
		params = ['r', options.schema || 'public'];

	return db.any(sql, params)
		.then((rows) => {
			// write table DDL and data
			let tables = [];

			if (rows.length) {
				let schema, table, columns;

				for (let row of rows) {
					if (row.column_id === 1) {
						if (table) {
							tables.push({ schema: schema, table: table, columns: columns });
						}

						schema = row.schema;
						table = row.table_name;
						columns = [];
					}

					let column = row;
					delete column.schema;
					delete column.table_name;
					columns.push(column);
				}

				tables.push({ schema: schema, table: table, columns: columns });
			}

			return tables.reduce((p, tableDef) => p.then(() => writeTable(tableDef.schema, tableDef.table, tableDef.columns) ), Promise.resolve());
		});
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

// example connection string
// postgres://john:pass123@localhost:5432/products

const pgp = require('pg-promise')();
const QueryStream = require('pg-query-stream');
const through = require('through');

const db = pgp(url);

writeTables()
	.then(function() {
		if (out !== process.stdout) {
			out.end(function(err) {
				process.exit(0);
			});
		}  else {
			process.exit(0);
		}
	})
	.catch(function(e) {
     console.error(e);
     process.exit(1);
});
