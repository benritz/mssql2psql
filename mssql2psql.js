"use strict"

var sql = require('mssql');

var out = process.stdout;

/**
 * Generate table defs.
 */
function generateTableDefs() {
    return sql.query`select t.name as table_name, c.column_id, c.name as column_name, c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, ty.name as type from sys.tables t, sys.columns c, sys.types ty where t.object_id = c.object_id and c.user_type_id = ty.user_type_id order by t.name asc, t.object_id asc, c.column_id asc`.then(function(rows) {
    		var table, columns;

			var writeDef = function() { out.write(`drop table if exists ${table};\n\ncreate table ${table}\n(\n${columns}\n);\n\n`); };

			if (rows.length) {
				for (let row of rows) {
					if (row.column_id === 1) {
						if (table) {
							writeDef.call();
						}

						table = row.table_name;
						columns = "";
					} else {
						columns += ",\n";
					}
				
					columns += row.column_name + " ";

					if (row.is_identity) {
						columns += "serial";
					} else if (row.type === "varchar" || row.type === "nvarchar") {
						if (row.max_length === -1) {
							columns += "text";
						} else {
							var maxLen = row.max_length;
							if (row.type === "nvarchar") {
								maxLen /= 2;
							}
							columns += "varchar(" + (maxLen) + ")";
						}
					} else if (row.type === "datetime") {
						columns += "timestamp";
					} else if (row.type === "image") {
						columns += "bytea";
					} else if (row.type === "bit") {
						columns += "boolean";
					} else {
						columns += row.type;
					}
																																																																										  
					if (!row.is_nullable) {
						columns += " not";
					}
					columns += " null";
				}
			
				writeDef.call();
			}
    }).catch(function(e) {
         console.error(e);
         });
}

/**
 * Generate foreign key defs.
 */
function generateForeignKeyDefs() {
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

			var writeDef = function() { out.write(`alter table ${parentTable} add constraint ${key} foreign key (${parentColumns}) references ${referencedTable} (${referencedColumns});\n`); };

			for (let row of rows) {
				if (row.constraint_column_id === 1) {
					if (parentTable) {
						writeDef.call();
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

			writeDef.call();
		}
	});
}

/**
 * Generate primary key, unique key and index defs.
 */
function generateIndexDefs() {
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

			var writeDef = function() {
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
						writeDef.call();
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
			
			writeDef.call();
		}
	});
}

/**
 * Generate default defs.
 */
function generateDefaultDefs() {
	return sql.query`select
t.name as table_name, c.name as column_name, d.name as default_name, d.definition 
from 
sys.tables t, sys.columns c, sys.default_constraints d 
where 
t.object_id = c.object_id and t.object_id = d.parent_object_id and c.column_id = d.parent_column_id
order by t.name asc, t.object_id asc, c.column_id asc`.then(function(rows) {
		if (rows.length) {
			for (let row of rows) {
				var definition = row.definition;
				
				definition = definition.replace(/^\(\((\d+)\)\)$/, "$1");
				definition = definition.replace("(getdate())", "now()");

				out.write(`alter table ${row.table_name} alter column ${row.column_name} set default ${definition};\n`);
			}
		}
	});
}


sql.connect("mssql://" + process.argv[2]).then(function() {
	generateTableDefs().then(function() {
		generateIndexDefs().then(function() {
			generateForeignKeyDefs().then(function() {
				generateDefaultDefs().then(function() {
					process.exit(0);
				});
			});
		});
	});
}).catch(function(e) {
     console.error(e);

     process.exit(1);
});
