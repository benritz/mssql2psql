"use strict";

function writeTable(table, columns) {
	var f = function(resolve, reject) {
		// get columns def
		var columnDefs = "";

		for (let column of columns) {
			if (columnDefs)
				columnDefs += ",\n";

			columnDefs += column.column_name + " ";

			if (column.is_identity) {
				columnDefs += "serial";
			} else if (column.type === "varchar" || column.type === "nvarchar" || 
						column.type === "text" || column.type === "ntext" ||
						(column.type === "char" && column.max_length > 1)) {
				if (options.forceCaseInsensitive) {
					columnDefs += "citext";
				} else {
					if (column.max_length === -1) {
						columnDefs += "text";
					} else {
						var maxLen = column.max_length;
						if (column.type === "nvarchar") {
							maxLen /= 2;
						}
						columnDefs += "varchar(" + (maxLen) + ")";
					}
				}
			} else if (column.type === "datetime") {
				columnDefs += "timestamp";
			} else if (column.type === "image") {
				columnDefs += "bytea";
			} else if (column.type === "bit") {
				columnDefs += "boolean";
			} else {
				columnDefs += column.type;
			}
																																																																							  
			if (!column.is_nullable) {
				columnDefs += " not";
			}
			columnDefs += " null";
		}		

		// create streaming recordset
		// write table def when the recordset is opened and then write insert for each row
		// although this is done in a promise the ms sql tedious driver blocks other 
		// recordset streams until this one is finished so the output for a table is
		// sequential
		var request = new sql.Request();
		request.stream = true;
		
		request.query(`select * from ${table}`);

		request.on("recordset", function() {
			// write table def
			out.write(`/* -- ${table} -- */\ndrop table if exists ${table};\n\ncreate table ${table}\n(\n${columnDefs}\n);\n\n`);
		});

		var n = 0;

		request.on('row', function(row) {
			var values = "";

			Object.keys(row).forEach(function(field) {
				var value = row[field];

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
			});

			if (options.dataBatchSize === 1) {
				out.write(`insert into ${table} values (${values});\n`);
			} else {
				if (n % options.dataBatchSize === 0) {
					if (n !== 0) {
						out.write(";\n\n");
					}
					out.write(`insert into ${table} values (${values})`);
				} else {
					out.write(`\n,(${values})`);
				}
			}

			++n;
		});

		request.on('error', function(err) {
			reject(err);
		});

		request.on('done', function(affected) {
			out.write(";\n");

			if (n !== 0) {
				out.write("\n");
			}

			resolve();
		});
	};

	return new Promise(function(resolve, reject) { f(resolve, reject); });
}

function writeTables() {
    return sql.query`select t.name as table_name, c.column_id, c.name as column_name, c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, ty.name as type from sys.tables t, sys.columns c, sys.types ty where t.object_id = c.object_id and c.user_type_id = ty.user_type_id order by t.name asc, t.object_id asc, c.column_id asc`.then(function(rows) {
		out.write("/* --------------------- TABLES --------------------- */\n\n");

		if (options.forceCaseInsensitive) {
			// enable case insensitive extension
			out.write("-- enable case insensitive extension\n");
			out.write("create extension citext;\n\n");
		}

		// write table DDL and data
		var a = [];

		if (rows.length) {
			var table, columns;

			for (let row of rows) {
				if (row.column_id === 1) {
					if (table) {
						a.push(writeTable(table, columns));
					}

					table = row.table_name;
					columns = [];
				}

				var column = row;
				delete column.table_name;
				columns.push(column);
			}

			a.push(writeTable(table, columns));
		}

		return Promise.all(a);
    }).catch(function(e) {
		console.error(e);
	});
}

function writeSeqReset() {
	// create function to get the maximum value for a sequence's field
	// see http://stackoverflow.com/a/5943183/1095458
	out.write("-- function to find a sequence's field's maximum value, this is used to set the sequence's next value after the data is inserted\n");
	out.write("-- see http://stackoverflow.com/a/5943183/1095458");
	out.write(`create or replace function seq_field_max_value(oid) returns bigint
	volatile strict language plpgsql as  $$
	declare
	 tabrelid oid;
	 colname name;
	 r record;
	 newmax bigint;
	begin
	 for tabrelid, colname in select attrelid, attname
				   from pg_attribute
				  where (attrelid, attnum) in (
						  select adrelid::regclass,adnum
							from pg_attrdef
						   where oid in (select objid
										   from pg_depend
										  where refobjid = $1
												and classid = 'pg_attrdef'::regclass
										)
			  ) loop
		  for r in execute 'select max(' || quote_ident(colname) || ') from ' || tabrelid::regclass loop
			  if newmax is null or r.max > newmax then
				  newmax := r.max;
			  end if;
		  end loop;
	  end loop;
	  return newmax;
	end; $$ ;\n\n`);
	
	// set any sequence to the maximum value of the sequence's field
	out.write("-- set any sequence to the maximum value of the sequence's field\n");
	out.write("select relname, setval(oid, seq_field_max_value(oid)) from pg_class where relkind = 'S';\n\n");
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
fk.object_id asc, fkc.constraint_column_id asc`.then(function(rows) {
		if (rows.length) {
			var key, parentColumns, parentTable, referencedColumns, referencedTable;

			var f = function() { out.write(`alter table ${parentTable} add constraint ${key} foreign key (${parentColumns}) references ${referencedTable} (${referencedColumns});\n`); };

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

				parentColumns += row.parent_column;
				referencedColumns += row.referenced_column;
			}

			f();

			out.write("\n");
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
order by t.name asc, t.object_id asc, i.index_id asc, ic.index_column_id asc`.then(function(rows) {
		if (rows.length) {
			var indexName, table, columns, isPrimary, isUniqueConst, isUnique;

			var f = function() {
				if (isPrimary) {
					out.write(`alter table ${table} add constraint ${indexName} primary key (${columns});\n`);
				} else if (isUniqueConst) {
					out.write(`alter table ${table} add constraint ${indexName} unique (${columns});\n`);
				} else if (isUnique) {
					out.write(`create unique index ${indexName} on ${table} (${columns});\n`);
				} else {
					out.write(`create index ${indexName} on ${table} (${columns});\n`);
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

				columns += row.column_name;
			}
			
			f();

			out.write("\n");
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
order by t.name asc, t.object_id asc, c.column_id asc`.then(function(rows) {
		if (rows.length) {
			var table;
			for (let row of rows) {
				if (table && table !== row.table_name) {
					out.write("\n");
				}
			
				table = row.table_name;
				var definition = row.definition;
				
				definition = definition.replace(/^\(\((\d+)\)\)$/, "$1");
				definition = definition.replace("(getdate())", "now()");
				
				if (row.type === "bit") {
					if (definition === "1") {
						definition = "true";
					} else if (definition === "0") {
						definition = "false";
					}
				}

				out.write(`alter table ${table} alter column ${row.column_name} set default ${definition};\n`);
			}
			
			out.write("\n");
		}
	});
}

/**
 * Write view defs.
 */
function writeViews() {
	out.write("/* --------------------- VIEWS --------------------- */\n\n");

	return sql.query`select c.text from sysobjects o, syscomments c where c.id = o.id and o.name = 'net_search_history' and o.type = 'V' order by o.name asc`.then(function(rows) {
		if (rows.length) {
			for (let row of rows) {
				out.write(row.text.trim());
				out.write(";\n");
			}

			out.write("\n");
		}
	});
}

var minimist = require('minimist');

var options = minimist(process.argv.slice(2), { 
	boolean: ["forceCaseInsensitive"], 
	default: { "dataBatchSize": 100, "forceCaseInsensitive": true } });
var args = options._;
delete options._;

if (args.length === 0) {
	console.error("Missing the connection URL.");
	process.exit(1);
}

var url = args[0];

var out;

if (args.length === 1) {
	out = process.stdout;
} else {
	let fs = require('fs');
	out = fs.createWriteStream(args[1]);
}

var sql = require('mssql');

sql.connect(url)
	.then(writeTables)
	.then(writeSeqReset)
	.then(writeIndexes)
	.then(writeForeignKeyes)
	.then(writeDefaults)
	.then(writeViews)
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
