"use strict"

var sql = require('mssql');


function readTableMetadata(tableName) {
    return sql.query`select c.name, c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, ty.name as type from sys.tables t, sys.columns c, sys.types ty where t.object_id = c.object_id and c.user_type_id = ty.user_type_id and t.name = ${tableName} order by c.column_id asc`.then(function(rows) {
            var s = "";

            for (let row of rows) {
                if (s) {
                    s += ", \n";
                }

                s += row.name + " ";

                if (row.is_identity) {
                	s += "serial";
                } else if (row.type === "varchar" || row.type === "nvarchar") {
					if (row.max_length === -1) {
						s += "text";
					} else {
						var maxLen = row.max_length;
						if (row.type === "nvarchar") {
							maxLen /= 2;
						}
						s += "varchar(" + (maxLen) + ")";
					}
				} else if (row.type === "datetime") {
					s += "timestamp";
				} else if (row.type === "image") {
					s += "bytea";
				} else if (row.type === "bit") {
					s += "boolean";
				} else {
					s += row.type;
				}
                                                                                                                                                                                                                                                                                                          
                if (!row.is_nullable) {
                    s += " not";
                }
                s += " null";
            }

            s = "drop table if exists " + tableName + "; create table " + tableName + " (\n" + s + "\n);\n\n";
            console.log(s);
    }).catch(function(e) {
         console.error(e);
         });
}

function readForeignKeys() {
	return sql.query`select 
fk.name 'key_name', t.name 'parent_table', c.name 'parent_column', rt.name 'referenced_table', rc.name 'referenced_column', fkc.constraint_column_id 'constraint_index'
from 
sys.tables t, sys.tables rt, sys.columns c, sys.columns rc, sys.foreign_keys fk, sys.foreign_key_columns fkc 
where 
fk.object_id = fkc.constraint_object_id and 
t.object_id = fk.parent_object_id and fkc.parent_column_id = c.column_id and c.object_id = t.object_id and
rt.object_id = fk.referenced_object_id and fkc.referenced_column_id = rc.column_id and rc.object_id = rt.object_id
order by 
fk.object_id asc, fkc.constraint_column_id asc`.then(function(rows) {
		console.log(rows);
	});
}

sql.connect("mssql://" + process.argv[2]).then(function() {
	readForeignKeys();

/*	new sql.Request().query('select name from sys.tables').then(function(rows) {
		var a = [];

		for (let row of rows) {
		    a.push(readTableMetadata(row.name));
		}

		return Promise.all(a);
	}).then(function() {
			process.exit(0);
		});
*/
}).catch(function(e) {
     console.error(e);

     process.exit(1);
});
