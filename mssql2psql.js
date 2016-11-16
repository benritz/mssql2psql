"use strict"

var sql = require('mssql');


function readTableMetaData(tableName) {
    sql.query`select c.name, c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, ty.name as type from sys.tables t, sys.columns c, sys.types ty where t.object_id = c.object_id and c.user_type_id = ty.user_type_id and t.name = ${tableName} order by c.column_id asc`.then(function(rows) {
            var s = "";

            for (let row of rows) {
                if (s) {
                    s += ", \n";
                }
                                                                                                                                                                                                                                                                                                          
                s += row.name + " ";
                if (row.is_identity) {                                                                                                                                                                                                                                                                                              s += "serial";
                } else {
                    if (row.type === "varchar") {
                        s += "text";
                        if (row.max_length !== -1) {
                            s += "(" + (row.max_length) + ")";
                        }
                    } else if (row.type === "nvarchar") {
                        s += "text";
                        if (row.max_length !== -1) {
                            s += "(" + (row.max_length / 2) + ")";
                        }
                    } else {
                        s += row.type;
                    }
                }
                                                                                                                                                                                                                                                                                                          
                if (!row.is_nullable) {
                    s += " not";
                }
                s += " null";
            }

            s = "create table " + tableName + " (\n" + s + "\n);\n\n";
            console.log(s);
    }).catch(function(e) {
         console.error(e);
         });
}

sql.connect("mssql://" + process.argv[2]).then(function() {
	new sql.Request().query('select name from sys.tables').then(function(rows) {
		for (let row of rows) {
		    readTableMetaData(row.name);
		}
	});
}).catch(function(e) {
     console.error("something bad happened");
     console.error(e);

     process.exit(1);
});
