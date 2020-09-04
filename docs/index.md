# HDBTCL

## Get Started

Assuming **hdbtcl** was installed outside of TCL - let's say both its dynamic library and `pkgIndex.tcl`
were copied into `/usr/local/lib/tcl/hdbtcl` - then that directory would have to be added to `auto_path`
by the application:
```tcl
lappend ::auto_path /usr/local/lib/tcl/hdbtcl
```

After that TCL knows where to find the package and an application would `require` it:
```tcl
package require hdbtcl
```

Once the package is successfully loaded the global `hdb` command is made available.

## Connecting to Database
```tcl
set server "localhost:39041"
set user   "JoeUser"
set pass   "S3cr3tW0rd"

set conn [hdb connect -serverNode $server -uid $user -pwd $pass]
```
> **Note** that for a single tenant databases (like HXE) database name is not required. Otherwise`-databaseName` should be provided as well.

`hdb connect` returns a command object that will be used for all subsequent interactions with the database.

The SAP HANA Client Interface Programming Reference also lists [SQLDBC Connection Properties][1] that
can be used to configure the session at the time when a connection to the database is established.

## Closing Database Connection
**hdbtcl** will close the connection automatically when the the command object retruned by `hdb connect` is no longer used.
For example:
```tcl
proc connect_query_and_disconnect { sql args } {
    set conn [hdb connect {*}$args]
    # ...
    # execute the query
    # ...
    return $result
}
```
When this procedure returns, application will no longer reference the command object that was kept by the procedure in the `conn` variable,
thus TCL will delete the command object and the deletion will trigger command cleanup which will make **hdbtcl** close the database connection.

However, if there is a need to close a connection explictly, this can be done via the `close` method. For example:
```tcl
set conn [hdb connect -serverNode $server -uid $user -pwd $pass]
# ...
# do stuff
# ...
$conn close
```
This will terminate the database connection, however it'll leave the command object intact, which after `close` will be invalid.

## Connected Session Configuration
When **hdbtcl** connects to the database the established session uses a default configuration:
- AUTOCOMMIT mode is off, so calling the commit or rollback is required for each transaction.
- The transaction isolation level is "READ COMMITTED"

These settings can be changed via `configure` method. For example:
```tcl
$conn configure -autocommit on -isolation "REPEATABLE READ"
```
The valid options for `configure` are:
- `-autocommit` - sets the AUTOCOMMIT mode to be on or off. When the AUTOCOMMIT mode is set to on, all statements are committed after they execute. They cannot be rolled back.
- `-isolation`  - sets the transaction isolation level. The possible options are "READ COMMITTED", "REPEATABLE READ" and "SERIALIZABLE".

## Querying Current Connection Configuration
The current state of the AUTOCOMMIT mode can be queried using `cget` method. For example:
```tcl
set is_autocommit [$conn cget -autocommit]
```
> **Note** that `-isolation` is a write-only option and cannot be queried via `cget`

## Setting Session-Specific Client Information
The `set` method allows setting and retrieving [session-specific client information][2] (name and value pair). For example:
```tcl
set current_app_name [$conn set APPLICATION]
$conn set APPLICATION "tclapp"
```

## Transactions
If the session AUTOCOMMIT mode is off each transaction must be explicitly committed or rolled back.

### Commit
```tcl
$conn commit
```
Commits the current transaction.

### Rollback
```tcl
$conn rollback
```
Rolls back the current transaction.

## SQL Execution

### Prepare Statement for Execution
Before the statement can be executed it must be prepared for execution. For example:
```tcl
set stmt [$conn prepare "INSERT INTO employees (employee_id, first_name, last_name) VALUES (?, ?, ?)"]
```
`prepare` method returns a statement object that will be used later to execute the SQL and fetch the results
of the execution. Explicit prepare is especially useful if the same SQL needs to be executed multiple times
with different parameters. For example:
```tcl
set stmt [$conn prepare "INSERT INTO employees (employee_id, first_name, last_name) VALUES (?, ?, ?)"]
foreach employee_info $employee_list {
    lassign $employee_info id first_name last_name
    $stmt execute $id $first_name $last_name
}
```

### Prepare And Execute a Statement
To execute a one-off statement the connection's `execute` method can be used. For example:
```tcl
$conn execute "
    CREATE TABLE employees (
        id          INT NOT NULL PRIMARY KEY,
        first_name  NVARCHAR(50) NOT NULL,
        last_name   NVARCHAR(50) NOT NULL
    )
"
```
It returns the statement object (this is not used in the example above) primarily to access returned results but
also to execute the same statement with different arguments if necessary. For example:
```tcl
set stmt [$conn execute "SELECT Count(*) FROM objects WHERE schema_name = ?" "SYS"]
# fetch the returned "count" from the result set
$stmt execute "SYSTEM"
# fetch the new "count"
```

> Internally `$conn execute` `prepare`s a statement for execution first and then `execute`s it.

### Closing The Statement
The statement command object can be closed explicitly when it is no longer needed. For example:
```tcl
$stmt close
```
However, usually calling `close` explicitly is usually not required becuase **hdbtcl** will close the statement
automatically when TCL is no longer references the command object.

### Execute The Prepared Statement
```tcl
$stmt execute ?arg arg ...?
```
This will execute the prepared statement using provided arguments for the parameter placeholders. For example:
```tcl
set stmt [$conn prepare "INSERT INTO employees (employee_id, first_name, last_name) VALUES (?, ?, ?)"]
$stmt execute 2 "Hasso" "Plattner"
```
> **Note** that the number of arguments must match the number of parameter placeholders in the prepared SQL.

### Fetching The Returned Results
```tcl
$stmt fetch row
```
This will fetch the next row from the returned result set and will store it in the `row` variable. The stored row is
a list where each element represents a projection column. `fetch` returns `true` if the row was fetched and stored
and `false` if result set has no more rows. For example:
```tcl
set stmt [$conn execute "SELECT employee_id, first_name, last_name FROM employees"]
while { [$stmt fetch row] } {
    lassign $row id first_name last_name
    # do something with the employeee info...
}
```

### Retrieve Statement Metadata
```tcl
$stmt get -prop
```
Will return a value of the specified statement property.

#### Number of Columns
`-numcols` or `-numcolumns` returns the number of columns in the SELECT's projection. Example:
```tcl
set stmt [$conn prepare "SELECT * FROM employees WHERE employee_id BETWEEN ? AND ?"]
set num_columns [$stmt get -numcols]
```

#### Column Names
`-colnames` or `-columnnames` returns the list of column names used in the SELECT's projection. Example:
```tcl
set stmt [$conn execute "SELECT * FROM employees WHERE employee_id BETWEEN ? AND ?" 1 99]
set cols [$stmt get -colnames]
# Convert fetched rows into the array
while { [$stmt fetch row] } {
    foreach name $cols value $row {
        set data($name) $value
        # pass array to a procedure that expects data in an array
    }
}
```

#### Column Info
```tcl
$stmt get -colinfo|-columninfo $column_number
```
`-colinfo` or `-columninfo` returns information about the specified column. The column info is returned
as a dictionary with the following keys:
- `owner`     - The name of the owner
- `table`     - The name of the table
- `name`      - The alias or the name of the column
- `type`      - The native type of the column in the database
- `size`      - The maximum size a data value in this column can take
- `nullable`  - Indicates whether a value in the column can be null
- `precision` - The precision
- `scale`     - The scale

Example:
```tcl
set col_num 0
set col_info [$stmt get -columninfo $col_num]
puts "Column [dict get $col_info name] is [dict get $col_info type]"
```

#### Number of Rows in The Result Set
```tcl
$stmt get -numrows
```
Returns the *approximate* number of rows in the result set.

#### Number of Affected Rows
```tcl
$stmt get -numaffectedrows
```
Returns the number of rows affected by execution of the statement that was executed successfully with no result set returned.
For example after an INSERT, UPDATE or DELETE statement was executed.
```tcl
set stmt [$conn execute "UPDATE employees SET first_name = InitCap(first_name) WHERE first_name != InitCap(first_name)"]
set num_updated [$stmt get -numaffectedrows]
```

#### Retrieve a Print Line
```tcl
$stmt get -printline varname
```
Retrieves a line printed during execution of a stored procedure. The initial call after execution of the stored procedure will
retrieve the first line printed. If the print line is successfully retrieved, it is removed from the queue and subsequent calls
will retrieve following print lines (if available). `get -printline` returns true if the printed line was retrieved and false
otherwise.

Example:
```sql
CREATE PROCEDURE test_proc AS
BEGIN
    USING SqlScript_Print AS print;
    SELECT 1 FROM dummy;
    print:print_line('Hello, World!');
    SELECT 2 FROM dummy;
    print:print_line('Hello World, Again!');
END
```

```tcl
set stmt [$conn execute "CALL test_proc"]
while { [$stmt get -printline line] } {
    puts $line
}
```

#### Retrieve Multiple Result Sets
If a query (such as a call to a stored procedure) returns multiple result sets, then `nextresult` method can be used to advance
from the current result set to the next. `nextresult` returns true if the statement successfully advances to the next result set
and false otherwise. Example:
```tcl
set stmt [$conn execute "CALL test_proc"]
set stmt_has_results 1
while { $stmt_has_results } {
    while { [$stmt fetch row] } {
        # process columns from the fetched row
    }
    set stmt_has_results [$stmt nextresult]
}
```

### Working With LOBs

#### Fetching LOBs From a Result Set

If a column in the SELECT's projection is a LOB, `fetch` will try, memory permitting, to fetch the entire
LOB into a string or a binary, depending on the column type. Example:
```tcl
set stmt [$conn execute "SELECT name, text_lob, binary_lob FROM some_table WHERE id = ?" $record_id]
# Assuming "id" is a primary key we'll expect a single row result set
if { [$stmt fetch row] } {
    lassign $row name text data
    # Here "text" is a string containing the entire character LOB
    # And "data" is a binary containing the entire binary LOB
}
```
While this works for some use cases, it might not be feasible for very large LOBs. Large LOBs can be fetched piece by
piece via custom LOB read callbacks. For example:
```tcl
proc read_lob { read_state lob_chunk col_num col_name table_name table_owner } {
    # Where:
    #   read_state  - Current reading state. Can be anything. Initial value is {}.
    #                 Next time read_lob is called read_state will have the value that
    #                 the previous call to read_lob returned.
    #   lob_chunk   - Current piece of LOB data. It'll be a string or a binary depending
    #                 on the LOB type.
    #   col_num     - LOB column number in the result set row.
    #   col_name    - LOB column name
    #   table_name  - Table name from which LOB is retrieved.
    #   table_owner - LOB table schema.

    # This procedure returns modified state to be passed to the call
    # that reads the next LOB piece.
    return $read_state
}

while { [$stmt fetch row -lobreadcommand read_lob] } {
    # non LOB columns will be retrieved by fetch and stored into the "row" variable
    # LOB columns in the returned "row" will contain the final "read state"
}
```
The default initial `read_state` for the `-lobreadcommand` is {} it can be set to a different value via `-lobreadinitialstate`.
For example:
```tcl
set name_prefix "temp_lob_file"
while { [$stmt fetch row -lobreadcommand read_lob_info_file -lobreadinitialstate $name_prefix] } {
    # process columns from the row
}
```

#### Loading OUT LOBs

When a LOB is an OUT argument of the stored procedure or an anonymous block it can be retrieved into a variable:
```tcl
set stmt [$::conn prepare "
    DO (IN p_id INT => ?, OUT p_lob NCLOB => ?)
    BEGIN
        SELECT text_lob INTO p_lob FROM some_table WHERE id = p_id;
    END
"]
$stmt execute $id text
```
or into an open writable channel:
```tcl
set stmt [$::conn prepare "
    DO (IN p_id INT => ?, OUT p_lob NCLOB => ?)
    BEGIN
        SELECT text_lob INTO p_lob FROM some_table WHERE id = p_id;
    END
"]
set lob_out [open test_data.txt w]
$stmt execute $id $lob_out
close $lob_out
# The content of the text_lob is now saved in the test_data.txt
```

#### LOB Input Arguments

A LOB data can be sent to the database from the memory object - a string or a binary:
```tcl
set quote "Success is not final; failure is not fatal: It is the courage to continue that counts."
set stmt [$::conn prepare "INSERT INTO some_table (id, text_lob) VALUES (?,?)"]
$stmt execute $id $quote
```
Or the LOB input data can be provided via an open readable channel:
```tcl
set stmt [$::conn prepare "UPDATE some_table SET text_lob = ? WHERE id = ?"]
set src [open test_data.txt r]
$stmt execute $src $id
close $src
```

[1]: <https://help.sap.com/viewer/0eec0d68141541d1b07893a39944924e/2.0.04/en-US/4fe9978ebac44f35b9369ef5a4a26f4c.html#loio4fe9978ebac44f35b9369ef5a4a26f4c__section_o3j_mpv_j1b>
[2]: <https://help.sap.com/viewer/0eec0d68141541d1b07893a39944924e/2.0.04/en-US/e90fa1f0e06e4840aa3ee2278afae16b.html>
