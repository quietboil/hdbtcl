# HDBTCL

Hdbtcl is an extension to the Tcl language that provides access to SAP HANA database server. Hdbtcl uses C-level DBCAPI to interface with the HANA database.

## Example

```tcl
# Need to tell tcl where hdbtcl can be loaded from.
# Thus, unless it installed in one of the predefined locations
# where packages are loaded from, ...
lappend ::auto_path /usr/local/lib/tcl/hdbtcl

package require hdbtcl

# Connect to the database
# Note that for a single tenant databases (like HXE) database name
# is not required. Otherwise -databaseName should be provided as well.
set conn [hdb connect -serverNode localhost:39041 -uid JoeUser -pwd S3cr3tW0rd]

# Reconfigure the connection to auto-commit all statements.
# The default mode is "no" and requires explicit commit or rollback.
$conn configure -autocommit yes

# Execute a one-off statement
$conn execute "
    CREATE TABLE employees (
        id          INT NOT NULL PRIMARY KEY,
        first_name  NVARCHAR(50) NOT NULL,
        last_name   NVARCHAR(50) NOT NULL
    )
"

# Prepare a parameterized statement for repeatable executions
set stmt [$conn prepare "
    INSERT INTO employees (id, first_name, last_name) VALUES (?, ?, ?)
"]

# Execute INSERT multiple times
foreach { id fn ln } { 2 "Hasso" "Plattner" } {
    $stmt execute $id $fn $ln
}

# Disconnet from the database.
# Note that this is not really required as hdbtcl will close the connection
# and all open statements for that connection when the connection command is
# no longer referenced.
$conn close
```

## Building

### Windows

#### Prerequisites

To compile `hdbtcl` on Windows tcl and gcc must be present. `hdbtcl` build was tested
with tcl built from sources, ActiveTcl and Thomas Perschak's binary distributions of
tcl 8.6. MinGW-W64 8.1.0 gcc (x86_64-win32-seh) was used to compile the extension.

#### Building HDBTCL

`win` directory contains the Makefile to build and test the extension. Before the build
the `local.mk` file, that Makefile will include, should be created to describe the build
and test environment:
```Makefile
DBCAPI_INCLUDE_DIR := C:/Apps/SAP/hdbclient/node/src/h
TCL   := C:/Apps/tcl86
MINGW := C:/Apps/mingw
CP    := cp
RM    := rm -f

HDBTCLTESTNODE := localhost:39041
HDBTCLTESTUSET := TestUser
HDBTCLTESTPASS := P4ssW0rd
export HDBCAPILIB := C:/Apps/SAP/hdbclient/libdbcapiHDB.dll
```
Where:
* `DBCAPI_INCLUDE_DIR` - directory where DBCAPI.h can be found.
* `TCL` - root directory of the tcl installation.
* `MINGW` - root directory where the MinGW gcc compiler installation.
* `CP` - local "copy" command. Might need to be set to `copy`.
* `RM` - local "delete" command. Might need to be set to `del /q`.
* `HDBTCLTESTNODE` - HDB server address.
* `HDBTCLTESTUSER` - HDB user name the test script will use to connect to HDB.
* `HDBTCLTESTPASS` - the password for the above account.
* `HDBCAPILIB` - full path to the `libdbcapiHDB.dll`.

> **Note** that `HDBCAPILIB` is only necessary if `libdbcapiHDB.dll` is not on the `PATH`.
> `hdbtcl` then loads it using `HDBCAPILIB` environment variable (hence the `export` BTW).
> Usually HDB client directory is not added to the PATH, therefore `HDBCAPILIB` would be
> routinely required.

Once `local.mk` is created, execute `make` to build or `make test` to build and run the `hdbtcl` test suite.

### Unix

`unix` directory contains the Makefile to build and test the extension. Before the build
the `local.mk` file, that Makefile will include, should be created to describe the build
and test environment:
```Makefile
DBCAPI_INCLUDE_DIR := /opt/sap/hdbclient/node/src/h

HDBTCLTESTNODE := hdbserver:39041
HDBTCLTESTUSER := TestUser
HDBTCLTESTPASS := P4ssW0rd
export HDBCAPILIB := /opt/sap/hdbclient/libdbcapiHDB.so
```
Where:
* `DBCAPI_INCLUDE_DIR` - directory where DBCAPI.h can be found.
* `HDBTCLTESTNODE` - HDB server address.
* `HDBTCLTESTUSER` - HDB user name the test script will use to connect to HDB.
* `HDBTCLTESTPASS` - the password for the above account.
* `HDBCAPILIB` - full path to the `libdbcapiHDB.so`.

> **Note** that `HDBCAPILIB` is only necessary if `libdbcapiHDB.so` is not in one of the
> `ld.so.conf` directories. `hdbtcl` then loads it using `HDBCAPILIB` environment variable
> (hence the `export` BTW). Usually HDB client directory is not added to the `ld.so.conf`,
> therefore `HDBCAPILIB` would be routinely required.

Once `local.mk` is created, execute `make` to build or `make test` to build and run the `hdbtcl` test suite.

## Installation

`hdbtcl` dynamic library and the accompanying `pkgIndex.tcl` can be added to one of the initial `auto_path` directories.
In that case `pkgIndex.tcl` would need to be merged with the `pkgIndex` at that location. Alternatively, `hdbtcl`
dynamic library and the `pkgIndex.tcl` can be saved anywhere and that directory added to `auto_path` by the application
that uses `hdbtcl`.

## Documentation

The complete [documentation][1] of the API offered by `hdbtcl` can be found in the [docs](docs) directory.

## Final Note

It appears that at the moment 32-bit Windows version of `libdbcapiHDB.dll` might have some issues processing LOB data.
None of the LOB tests in `hdbtcl` test suite were successfull. The same tests complete successfully in 64-bit
environments (on both Windows and Linux).

[1]: <https://quietboil.github.io/hdbtcl>
