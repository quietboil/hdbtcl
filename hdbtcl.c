#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdarg.h>
#include <memory.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <limits.h>
static char *
itoa(int value, char * result, int base) {
    // constraints:
    // - only base 10 is used by this implementation
    // - output is never > 12 characters including terminating \0
    char buf[12];
    char * out = buf + sizeof(buf);
    *--out = '\0';
	do {
		*--out = "0123456789" [value % base];
		value /= base;
	} while ( value != 0 && out > buf );
	return memcpy(result, out, sizeof(buf) - (buf - out));
}
#endif

#include <tcl.h>
#include <DBCAPI.h>

/**
 * Collection of DBCAPI functions used by this interface.
 */
static struct dbcapi {
    dbcapi_bool             ( * init )( const char * app_name, dbcapi_u32 api_version, dbcapi_u32 * version_available );
    void                    ( * fini )();
    dbcapi_connection *     ( * new_connection )();
    void                    ( * free_connection )( dbcapi_connection * dbcapi_conn );
    dbcapi_bool             ( * connect2 )( dbcapi_connection * dbcapi_conn );
    dbcapi_bool             ( * disconnect )( dbcapi_connection * dbcapi_conn );
    dbcapi_bool             ( * set_connect_property )( dbcapi_connection * dbcapi_conn, const char * property, const char * value );
    dbcapi_bool             ( * set_clientinfo )( dbcapi_connection * dbcapi_conn, const char * property, const char * value );
    const char *            ( * get_clientinfo )( dbcapi_connection * dbcapi_conn, const char * property );
    dbcapi_bool             ( * set_transaction_isolation )( dbcapi_connection * dbcapi_conn, dbcapi_u32 isolation_level );
    dbcapi_bool             ( * set_autocommit )( dbcapi_connection * dbcapi_conn, dbcapi_bool mode );
    dbcapi_bool             ( * get_autocommit )( dbcapi_connection * dbcapi_conn, dbcapi_bool * mode );
    dbcapi_bool             ( * commit )( dbcapi_connection * dbcapi_conn );
    dbcapi_bool             ( * rollback )( dbcapi_connection * dbcapi_conn );
    size_t                  ( * error_length )( dbcapi_connection * dbcapi_conn );
    dbcapi_i32              ( * error )( dbcapi_connection * dbcapi_conn, char * buffer, size_t size );
    dbcapi_stmt *           ( * prepare )( dbcapi_connection * dbcapi_conn, const char * sql_str );
    dbcapi_bool             ( * reset )( dbcapi_stmt * dbcapi_stmt );
    void                    ( * free_stmt )( dbcapi_stmt * dbcapi_stmt );
    dbcapi_i32              ( * num_params )( dbcapi_stmt * dbcapi_stmt );
    dbcapi_bool             ( * describe_bind_param )( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, dbcapi_bind_data * param );
    dbcapi_bool             ( * bind_param )( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, dbcapi_bind_data * param );
    dbcapi_bool             ( * get_bind_param_info )( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, dbcapi_bind_param_info * info );
    dbcapi_bool             ( * send_param_data )( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index, char * buffer, size_t size );
    dbcapi_i32              ( * get_param_data )( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 param_index, size_t offset, void * buffer, size_t size );
    dbcapi_bool             ( * finish_param_data )( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 index );
    dbcapi_bool             ( * execute )( dbcapi_stmt * dbcapi_stmt );
    dbcapi_bool             ( * fetch_next )( dbcapi_stmt * dbcapi_stmt );
    dbcapi_bool             ( * get_next_result )( dbcapi_stmt * dbcapi_stmt );
    dbcapi_i32              ( * affected_rows )( dbcapi_stmt * dbcapi_stmt );
    dbcapi_i32              ( * num_cols )( dbcapi_stmt * dbcapi_stmt );
    dbcapi_i32              ( * num_rows )( dbcapi_stmt * dbcapi_stmt );
    dbcapi_bool             ( * get_column )( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, dbcapi_data_value * buffer );
    dbcapi_bool             ( * get_column_info )( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, dbcapi_column_info * buffer );
    dbcapi_i32              ( * get_data )( dbcapi_stmt * dbcapi_stmt, dbcapi_u32 col_index, size_t offset, void * buffer, size_t size );
    dbcapi_retcode          ( * get_print_line )( dbcapi_stmt * dbcapi_stmt, const dbcapi_i32 host_type, void * buffer, size_t * length_indicator, size_t buffer_size, const dbcapi_bool terminate );
} dbcapi;

#ifdef _WIN32
# define LIB_EXT "dll"
#else
# include <dlfcn.h>
/* assume we are running on a UNIX platform */
# ifdef __APPLE__
#  define LIB_EXT "dylib"
# else
#  define LIB_EXT "so"
# endif
#endif

static void *
load_lib ( const char * filename )
{
#ifdef _WIN32
    return LoadLibrary( filename );
#else
    return dlopen( filename, RTLD_LAZY );
#endif
}

static void
free_lib ( void * handle )
{
#ifdef _WIN32
    FreeLibrary( (HMODULE) handle );
#else
    dlclose( handle );
#endif
}

static void *
find_sym ( void * lib_handle, const char * sym_name )
{
#ifdef _WIN32
    return (void *) GetProcAddress( (HMODULE) lib_handle, sym_name );
#else
    return dlsym( lib_handle, sym_name );
#endif
}

#define INIT_FN( handle, sym ) do {                     \
    dbcapi.sym = find_sym( handle, "dbcapi_" #sym );    \
    if ( dbcapi.sym == NULL ) {                         \
        free_lib( handle );                             \
        dbcapi.init = NULL;                             \
        return false;                                   \
    }                                                   \
} while (0)

static bool
init_dbcapi ( const char * apilib_filename )
{
    void * lib = load_lib( apilib_filename != NULL ? apilib_filename : "libdbcapiHDB." LIB_EXT );
    if ( lib == NULL ) {
        return false;
    }
    INIT_FN( lib, init );
    INIT_FN( lib, fini );
    INIT_FN( lib, new_connection );
    INIT_FN( lib, free_connection );
    INIT_FN( lib, connect2 );
    INIT_FN( lib, disconnect );
    INIT_FN( lib, set_connect_property );
    INIT_FN( lib, set_clientinfo );
    INIT_FN( lib, get_clientinfo );
    INIT_FN( lib, set_transaction_isolation );
    INIT_FN( lib, set_autocommit );
    INIT_FN( lib, get_autocommit );
    INIT_FN( lib, commit );
    INIT_FN( lib, rollback );
    INIT_FN( lib, error_length );
    INIT_FN( lib, error );
    INIT_FN( lib, prepare );
    INIT_FN( lib, reset );
    INIT_FN( lib, free_stmt );
    INIT_FN( lib, num_params );
    INIT_FN( lib, describe_bind_param );
    INIT_FN( lib, bind_param );
    INIT_FN( lib, get_bind_param_info );
    INIT_FN( lib, send_param_data );
    INIT_FN( lib, get_param_data );
    INIT_FN( lib, finish_param_data );
    INIT_FN( lib, execute );
    INIT_FN( lib, fetch_next );
    INIT_FN( lib, get_next_result );
    INIT_FN( lib, affected_rows );
    INIT_FN( lib, num_cols );
    INIT_FN( lib, num_rows );
    INIT_FN( lib, get_column );
    INIT_FN( lib, get_column_info );
    INIT_FN( lib, get_data );
    INIT_FN( lib, get_print_line );

    return true;
}

// HANA can return up to 4 bytes per character
#if TCL_UTF_MAX < 4
#define BYTES_PER_CODEPOINT TCL_UTF_MAX
#else
#define BYTES_PER_CODEPOINT 4
#endif

#define NUM_NATIVE_TYPES 0x50

/**
 * The constant string used by the module to return literals.
 */
typedef struct const_literals {
    Tcl_Obj *  owner;
    Tcl_Obj *  table;
    Tcl_Obj *  name;
    Tcl_Obj *  type;
    Tcl_Obj *  size;
    Tcl_Obj *  nullable;
    Tcl_Obj *  precision;
    Tcl_Obj *  scale;
    Tcl_Obj *  dt[NUM_NATIVE_TYPES];
    Tcl_Obj *  dt_unknown;
    Tcl_Obj *  dt_notype;
} Hdbtcl_Literals;

/**
 * Creates TCL strings with all known literals.
 */
static void
Hdbtcl_InitLiterals (Hdbtcl_Literals * lit_ptr)
{
    Tcl_IncrRefCount(lit_ptr->owner      = Tcl_NewStringObj("owner",     -1));
    Tcl_IncrRefCount(lit_ptr->table      = Tcl_NewStringObj("table",     -1));
    Tcl_IncrRefCount(lit_ptr->name       = Tcl_NewStringObj("name",      -1));
    Tcl_IncrRefCount(lit_ptr->type       = Tcl_NewStringObj("type",      -1));
    Tcl_IncrRefCount(lit_ptr->size       = Tcl_NewStringObj("size",      -1));
    Tcl_IncrRefCount(lit_ptr->nullable   = Tcl_NewStringObj("nullable",  -1));
    Tcl_IncrRefCount(lit_ptr->precision  = Tcl_NewStringObj("precision", -1));
    Tcl_IncrRefCount(lit_ptr->scale      = Tcl_NewStringObj("scale",     -1));
    Tcl_IncrRefCount(lit_ptr->dt_notype  = Tcl_NewStringObj("NOTYPE",    -1));
    Tcl_IncrRefCount(lit_ptr->dt_unknown = Tcl_NewStringObj("UNKNOWN",   -1));
    for (int i = 0; i < NUM_NATIVE_TYPES; i++ ) {
        lit_ptr->dt[i] = lit_ptr->dt_unknown;
    }
    lit_ptr->dt[DT_NULL]                = Tcl_NewStringObj("NULL",                -1);
    lit_ptr->dt[DT_TINYINT]             = Tcl_NewStringObj("TINYINT",             -1);
    lit_ptr->dt[DT_SMALLINT]            = Tcl_NewStringObj("SMALLINT",            -1);
    lit_ptr->dt[DT_INT]                 = Tcl_NewStringObj("INT",                 -1);
    lit_ptr->dt[DT_BIGINT]              = Tcl_NewStringObj("BIGINT",              -1);
    lit_ptr->dt[DT_DECIMAL]             = Tcl_NewStringObj("DECIMAL",             -1);
    lit_ptr->dt[DT_REAL]                = Tcl_NewStringObj("REAL",                -1);
    lit_ptr->dt[DT_DOUBLE]              = Tcl_NewStringObj("DOUBLE",              -1);
    lit_ptr->dt[DT_CHAR]                = Tcl_NewStringObj("CHAR",                -1);
    lit_ptr->dt[DT_VARCHAR1]            = Tcl_NewStringObj("VARCHAR1",            -1);
    lit_ptr->dt[DT_NCHAR]               = Tcl_NewStringObj("NCHAR",               -1);
    lit_ptr->dt[DT_NVARCHAR]            = Tcl_NewStringObj("NVARCHAR",            -1);
    lit_ptr->dt[DT_BINARY]              = Tcl_NewStringObj("BINARY",              -1);
    lit_ptr->dt[DT_VARBINARY]           = Tcl_NewStringObj("VARBINARY",           -1);
    lit_ptr->dt[DT_DATE]                = Tcl_NewStringObj("DATE",                -1);
    lit_ptr->dt[DT_TIME]                = Tcl_NewStringObj("TIME",                -1);
    lit_ptr->dt[DT_TIMESTAMP]           = Tcl_NewStringObj("TIMESTAMP",           -1);
    lit_ptr->dt[DT_TIME_TZ]             = Tcl_NewStringObj("TIME_TZ",             -1);
    lit_ptr->dt[DT_TIME_LTZ]            = Tcl_NewStringObj("TIME_LTZ",            -1);
    lit_ptr->dt[DT_TIMESTAMP_TZ]        = Tcl_NewStringObj("TIMESTAMP_TZ",        -1);
    lit_ptr->dt[DT_TIMESTAMP_LTZ]       = Tcl_NewStringObj("TIMESTAMP_LTZ",       -1);
    lit_ptr->dt[DT_INTERVAL_YM]         = Tcl_NewStringObj("INTERVAL_YM",         -1);
    lit_ptr->dt[DT_INTERVAL_DS]         = Tcl_NewStringObj("INTERVAL_DS",         -1);
    lit_ptr->dt[DT_ROWID]               = Tcl_NewStringObj("ROWID",               -1);
    lit_ptr->dt[DT_UROWID]              = Tcl_NewStringObj("UROWID",              -1);
    lit_ptr->dt[DT_CLOB]                = Tcl_NewStringObj("CLOB",                -1);
    lit_ptr->dt[DT_NCLOB]               = Tcl_NewStringObj("NCLOB",               -1);
    lit_ptr->dt[DT_BLOB]                = Tcl_NewStringObj("BLOB",                -1);
    lit_ptr->dt[DT_BOOLEAN]             = Tcl_NewStringObj("BOOLEAN",             -1);
    lit_ptr->dt[DT_STRING]              = Tcl_NewStringObj("STRING",              -1);
    lit_ptr->dt[DT_NSTRING]             = Tcl_NewStringObj("NSTRING",             -1);
    lit_ptr->dt[DT_LOCATOR]             = Tcl_NewStringObj("LOCATOR",             -1);
    lit_ptr->dt[DT_NLOCATOR]            = Tcl_NewStringObj("NLOCATOR",            -1);
    lit_ptr->dt[DT_BSTRING]             = Tcl_NewStringObj("BSTRING",             -1);
    lit_ptr->dt[DT_DECIMAL_DIGIT_ARRAY] = Tcl_NewStringObj("DECIMAL_DIGIT_ARRAY", -1);
    lit_ptr->dt[DT_VARCHAR2]            = Tcl_NewStringObj("VARCHAR2",            -1);
    lit_ptr->dt[DT_TABLE]               = Tcl_NewStringObj("TABLE",               -1);
    lit_ptr->dt[DT_ABAPSTREAM]          = Tcl_NewStringObj("ABAPSTREAM",          -1);
    lit_ptr->dt[DT_ABAPSTRUCT]          = Tcl_NewStringObj("ABAPSTRUCT",          -1);
    lit_ptr->dt[DT_ARRAY]               = Tcl_NewStringObj("ARRAY",               -1);
    lit_ptr->dt[DT_TEXT]                = Tcl_NewStringObj("TEXT",                -1);
    lit_ptr->dt[DT_SHORTTEXT]           = Tcl_NewStringObj("SHORTTEXT",           -1);
    lit_ptr->dt[DT_BINTEXT]             = Tcl_NewStringObj("BINTEXT",             -1);
    lit_ptr->dt[DT_ALPHANUM]            = Tcl_NewStringObj("ALPHANUM",            -1);
    lit_ptr->dt[DT_LONGDATE]            = Tcl_NewStringObj("LONGDATE",            -1);
    lit_ptr->dt[DT_SECONDDATE]          = Tcl_NewStringObj("SECONDDATE",          -1);
    lit_ptr->dt[DT_DAYDATE]             = Tcl_NewStringObj("DAYDATE",             -1);
    lit_ptr->dt[DT_SECONDTIME]          = Tcl_NewStringObj("SECONDTIME",          -1);
    lit_ptr->dt[DT_CLOCATOR]            = Tcl_NewStringObj("CLOCATOR",            -1);
    lit_ptr->dt[DT_BLOB_DISK_RESERVED]  = Tcl_NewStringObj("BLOB_DISK_RESERVED",  -1);
    lit_ptr->dt[DT_CLOB_DISK_RESERVED]  = Tcl_NewStringObj("CLOB_DISK_RESERVED",  -1);
    lit_ptr->dt[DT_NCLOB_DISK_RESERVE]  = Tcl_NewStringObj("NCLOB_DISK_RESERVE",  -1);
    lit_ptr->dt[DT_ST_GEOMETRY]         = Tcl_NewStringObj("ST_GEOMETRY",         -1);
    lit_ptr->dt[DT_ST_POINT]            = Tcl_NewStringObj("ST_POINT",            -1);
    lit_ptr->dt[DT_FIXED16]             = Tcl_NewStringObj("FIXED16",             -1);
    lit_ptr->dt[DT_ABAP_ITAB]           = Tcl_NewStringObj("ABAP_ITAB",           -1);
    lit_ptr->dt[DT_RECORD_ROW_STORE]    = Tcl_NewStringObj("RECORD_ROW_STORE",    -1);
    lit_ptr->dt[DT_RECORD_COLUMN_STORE] = Tcl_NewStringObj("RECORD_COLUMN_STORE", -1);
    for (int i = 0; i < NUM_NATIVE_TYPES; i++ ) {
        Tcl_IncrRefCount(lit_ptr->dt[i]);
    }
}

/**
 * Deletes all constant TCL strings created during module initialization.
 */
static void
Hdbtcl_DeleteLiterals (Hdbtcl_Literals * lit_ptr)
{
    if ( lit_ptr->owner     != NULL ) Tcl_DecrRefCount(lit_ptr->owner);
    if ( lit_ptr->table     != NULL ) Tcl_DecrRefCount(lit_ptr->table);
    if ( lit_ptr->name      != NULL ) Tcl_DecrRefCount(lit_ptr->name);
    if ( lit_ptr->type      != NULL ) Tcl_DecrRefCount(lit_ptr->type);
    if ( lit_ptr->size      != NULL ) Tcl_DecrRefCount(lit_ptr->size);
    if ( lit_ptr->nullable  != NULL ) Tcl_DecrRefCount(lit_ptr->nullable);
    if ( lit_ptr->precision != NULL ) Tcl_DecrRefCount(lit_ptr->precision);
    if ( lit_ptr->scale     != NULL ) Tcl_DecrRefCount(lit_ptr->scale);
    for (int i = 0; i < NUM_NATIVE_TYPES; i++ ) {
        if ( lit_ptr->dt[i] != NULL ) {
            Tcl_DecrRefCount(lit_ptr->dt[i]);
        }
    }
    if ( lit_ptr->dt_notype  != NULL ) Tcl_DecrRefCount(lit_ptr->dt_notype);
    if ( lit_ptr->dt_unknown != NULL ) Tcl_DecrRefCount(lit_ptr->dt_unknown);
}

/**
 * Returns a TCL string with the name of the native HANA type.
 */
static Tcl_Obj *
GetTypeName (Hdbtcl_Literals * lit_ptr, dbcapi_native_type type)
{
    if ( type < NUM_NATIVE_TYPES ) return lit_ptr->dt[type];
    if ( type == DT_NOTYPE ) return lit_ptr->dt_notype;
    return lit_ptr->dt_unknown;
}

/**
 * Internal module state.
 */
typedef struct hdbtcl_state {
    Tcl_HashTable *     open_connections;
    Tcl_Command         hdb_cmd;
    Hdbtcl_Literals     literals;
    Tcl_ObjType const * list_type;
} Hdbtcl_State;

static int Hdb_Cmd (Hdbtcl_State * hdbtcl_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[]);

/**
 * Deletes the module state.
 *
 * During module state deletion `hdbtcl` will closes all open connections and delete the `hdb` command.
 */
static void
Hdbtcl_DeleteState (Hdbtcl_State * hdbtcl_state_ptr, Tcl_Interp * interp)
{
    if ( hdbtcl_state_ptr == NULL ) return;

    hdbtcl_state_ptr->hdb_cmd = NULL;
    if ( hdbtcl_state_ptr->open_connections != NULL ) {
        Tcl_HashSearch iter;
        for (
            Tcl_HashEntry * entry = Tcl_FirstHashEntry(hdbtcl_state_ptr->open_connections, &iter);
            entry != NULL;
            entry = Tcl_NextHashEntry(&iter)
        ) {
            Tcl_Command conn_cmd = Tcl_GetHashValue(entry);
            Tcl_DeleteHashEntry(entry);
            Tcl_DeleteCommandFromToken(interp, conn_cmd);
        }
        Tcl_DeleteHashTable(hdbtcl_state_ptr->open_connections);
        ckfree(hdbtcl_state_ptr->open_connections);
    }
    Hdbtcl_DeleteLiterals(&hdbtcl_state_ptr->literals);
    ckfree((char *) hdbtcl_state_ptr);
}

/**
 * Creates and initializes the module state.
 *
 * The primary side-effect - the `hdb` command becomes available in the loading interpreter.
 */
static Hdbtcl_State *
Hdbtcl_InitState (Tcl_Interp * interp)
{
    Hdbtcl_State * hdbtcl_state_ptr = (Hdbtcl_State *) ckalloc(sizeof(Hdbtcl_State));
    if ( hdbtcl_state_ptr == NULL ) {
        Tcl_SetResult(interp, "cannot allocate memory for the module internal state", TCL_STATIC);
        return NULL;
    }
    memset(hdbtcl_state_ptr, 0, sizeof(Hdbtcl_State));
    hdbtcl_state_ptr->open_connections = ckalloc(sizeof(Tcl_HashTable));
    Tcl_InitHashTable(hdbtcl_state_ptr->open_connections, TCL_ONE_WORD_KEYS);
    Hdbtcl_InitLiterals(&hdbtcl_state_ptr->literals);

    hdbtcl_state_ptr->list_type = Tcl_GetObjType("list");

    hdbtcl_state_ptr->hdb_cmd = Tcl_CreateObjCommand(interp, "hdb", (Tcl_ObjCmdProc *) Hdb_Cmd, (ClientData) hdbtcl_state_ptr, (Tcl_CmdDeleteProc *) NULL);
    if ( hdbtcl_state_ptr->hdb_cmd == NULL ) {
        Tcl_SetResult(interp, "cannot create hdb command", TCL_STATIC);
        Hdbtcl_DeleteState(hdbtcl_state_ptr, interp);
        return NULL;
    }
    return hdbtcl_state_ptr;
}

/**
 * Internal connection state.
 */
typedef struct conn_state {
    dbcapi_connection * conn;
    Tcl_Command         conn_cmd;
    Tcl_HashTable *     open_statements;
    Hdbtcl_State *      hdbtcl_state_ptr;
    bool                connected;
} Conn_State;

/**
 * Destroys and deletes a connection state.
 */
static void
Conn_DeleteState (Conn_State * conn_state_ptr, Tcl_Interp * interp)
{
    if ( conn_state_ptr == NULL ) {
        return;
    }
    if ( conn_state_ptr->conn_cmd != NULL ) {
        // if connection is not being deleted because the module is being deleted (hdb_cmd is null when module is being deleted),
        // then remove the command from the module's set of open connections
        if ( conn_state_ptr->hdbtcl_state_ptr->hdb_cmd != NULL) {
            Tcl_HashEntry * entry = Tcl_FindHashEntry(conn_state_ptr->hdbtcl_state_ptr->open_connections, conn_state_ptr->conn_cmd);
            if ( entry != NULL ) {
                Tcl_DeleteHashEntry(entry);
            }
        }
        conn_state_ptr->conn_cmd = NULL;
    }
    if ( conn_state_ptr->open_statements != NULL ) {
        Tcl_HashSearch iter;
        for (
            Tcl_HashEntry * entry = Tcl_FirstHashEntry(conn_state_ptr->open_statements, &iter);
            entry != NULL;
            entry = Tcl_NextHashEntry(&iter)
        ) {
            Tcl_Command stmt_cmd = Tcl_GetHashValue(entry);
            Tcl_DeleteHashEntry(entry);
            Tcl_DeleteCommandFromToken(interp, stmt_cmd);
        }
        Tcl_DeleteHashTable(conn_state_ptr->open_statements);
        ckfree(conn_state_ptr->open_statements);
    }
    if ( conn_state_ptr->conn != NULL ) {
        if ( conn_state_ptr->connected ) {
            dbcapi.disconnect(conn_state_ptr->conn);
        }
        dbcapi.free_connection(conn_state_ptr->conn);
    }
    ckfree((char *) conn_state_ptr);
}

/**
 * Internal statement state.
 */
typedef struct stmt_state {
    dbcapi_stmt *       stmt;
    Tcl_Command         stmt_cmd;
    Conn_State *        conn_state_ptr;
} Stmt_State;

/**
 * Destroys and deletes a statement state.
 */
static void
Stmt_DeleteState (Stmt_State * stmt_state_ptr, Tcl_Interp * interp)
{
    if ( stmt_state_ptr == NULL ) {
        return;
    }
    if ( stmt_state_ptr->conn_state_ptr != NULL && stmt_state_ptr->stmt_cmd != NULL ) {
        // If the statement is being deleted explicitly and not because connection executes finalization clean up
        // (it is being deleted and it deletes all its statements), then remove statement entry from the set of
        // connection's open statements
        if ( stmt_state_ptr->conn_state_ptr->conn_cmd != NULL ) {
            Tcl_HashEntry * entry = Tcl_FindHashEntry(stmt_state_ptr->conn_state_ptr->open_statements, stmt_state_ptr->stmt_cmd);
            if ( entry != NULL ) {
                Tcl_DeleteHashEntry(entry);
            }
        }
        stmt_state_ptr->conn_state_ptr = NULL;
    }
    if ( stmt_state_ptr->stmt != NULL ) {
        dbcapi.free_stmt(stmt_state_ptr->stmt);
        stmt_state_ptr->stmt = NULL;
    }
    ckfree((char *) stmt_state_ptr);
}

/**
 * Reports DBCAPI errors as TCL errors.
 */
static void
SetErrorResult (Tcl_Interp * interp, dbcapi_connection * conn, const char * message, ...)
{
    va_list args;
    va_start(args, message);
    Tcl_AppendResult(interp, message, NULL);
    Tcl_AppendResultVA(interp, args);
    va_end(args);

    size_t msg_size = dbcapi.error_length(conn);
    char reason[msg_size];
    int code = dbcapi.error(conn, reason, msg_size);

    char reason_prefix[32];
    sprintf(reason_prefix, " - Code: %d Reason: ", code);

    Tcl_AppendResult(interp, reason_prefix, reason, NULL);
}

/**
 * Closes the statement.
 *
 * # Example
 * \code{.tcl}
 * $stmt close
 * \endcode
 *
 * \note After the stemenet is closed the stored command is not longer valid. Also, the explicit
 * `close` is not necessary. `hdbtcl` will close the statement automatically when all saved
 * references to it are gone.
 */
static int
Stmt_Close (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( Tcl_DeleteCommandFromToken(interp, stmt_state_ptr->stmt_cmd) != TCL_OK ) {
        Tcl_SetResult(interp, "Cannot delete statement handle", TCL_STATIC);
        return TCL_ERROR;
    }
    return TCL_OK;
}

/**
 * Type to store bound primitives.
 */
typedef union primitive_sql_value {
    double      double_value;
    int         int_value;
    Tcl_WideInt wideint_value;
    size_t      data_length;
} PrimitiveSqlValue;

/**
 * Helper function to report "expected N but got M" type of errors.
 */
static void
SetNumDiffErrorResult (Tcl_Interp * interp, const char * txt_begin, int num1, const char * txt_mid, int num2, const char * txt_end)
{
    char txt1[12], txt2[12];
    Tcl_AppendResult(interp, txt_begin, itoa(num1, txt1, 10), txt_mid, itoa(num2, txt2, 10), txt_end, NULL);
}

/**
 * Macro that help to avoid the multiline pattern of binding parameters.
 */
#define BIND_PARAM(stmt_state_ptr, interp, i, bind_ptr) do { \
    if ( !dbcapi.bind_param((stmt_state_ptr)->stmt, (i), (bind_ptr)) ) { \
        char num[12]; \
        SetErrorResult((interp), (stmt_state_ptr)->conn_state_ptr->conn, "Cannot bind SQL argument [", itoa((i), num, 10), "]", NULL); \
        return TCL_ERROR; \
    } \
} while (0)

/**
 * Binds statement arguments to the respective placeholders.
 */
static int
BindStmtArgs (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int argc, Tcl_Obj * const argv[], dbcapi_bool is_null[], PrimitiveSqlValue sql_args[])
{
    int num_params = dbcapi.num_params(stmt_state_ptr->stmt);
    if ( num_params < 0 ) {
        SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot determine the number of statement parameters", NULL);
        return TCL_ERROR;
    }
    if ( argc != num_params ) {
        SetNumDiffErrorResult(interp, "Expected ", num_params, " arguments but got ", argc, NULL);
        return TCL_ERROR;
    }

    if ( !dbcapi.reset(stmt_state_ptr->stmt) ) {
        SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot reset statement for execution", NULL);
        return TCL_ERROR;
    }

    for ( int i = 0; i < argc; i++ ) {
        dbcapi_bind_data bind;
        dbcapi_bind_param_info info;
        if (
            !dbcapi.describe_bind_param(stmt_state_ptr->stmt, i, &bind) ||
            !dbcapi.get_bind_param_info(stmt_state_ptr->stmt, i, &info)
        ) {
            char num[12];
            SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve information about SQL parameter [", itoa(i, num, 10), "]", NULL);
            return TCL_ERROR;
        }
        if ( bind.value.type == A_INVALID_TYPE ) {
            char num[12];
            Tcl_AppendResult(interp, "DBCAPI did not return the type of parameter [", itoa(i, num, 10), "],", NULL);
            if ( bind.name != NULL && bind.name[0] != '\0' ) {
                Tcl_AppendResult(interp, " (", bind.name, ")", NULL);
            }
            return TCL_ERROR;
        }

        if ( bind.value.buffer_size == INT32_MAX ) {
            // It's a LOB. The argument might be an object or an open channel or a pair of channels (for INOUT parameters)

            if ( bind.direction == DD_INPUT && argv[i]->bytes != NULL && 0 < argv[i]->length && argv[i]->length < 32 ) {
                const char * name = Tcl_GetString(argv[i]);
                int mode;
                Tcl_Channel ch = Tcl_GetChannel(interp, name, &mode);
                if ( ch != NULL ) {
                    // Check whether it is usable...
                    if ( ( mode & TCL_READABLE ) == 0 ) {
                        Tcl_AppendResult(interp, "Channel ", name, " must be open for reading", NULL);
                        return TCL_ERROR;
                    }
                    sql_args[i].data_length = INT32_MAX;
                    bind.value.length = &sql_args[i].data_length;
                    bind.value.buffer_size = 0;
                } else {
                    Tcl_ResetResult(interp);
                }
            } else if ( bind.direction == DD_INPUT_OUTPUT || bind.direction == DD_OUTPUT ) {
                // Output is always streamed
                sql_args[i].data_length = INT32_MAX;
                bind.value.length = &sql_args[i].data_length;
                bind.value.buffer_size = 0;
            }

            if ( bind.value.buffer_size == 0 ) {
                BIND_PARAM(stmt_state_ptr, interp, i, &bind);
                continue;
            }
        }

        // Set up buffers for primitives
        switch ( bind.value.type ) {
            case A_VAL32: case A_UVAL32: case A_VAL16: case A_UVAL16: case A_VAL8: case A_UVAL8:
                bind.value.type = A_VAL32;
                bind.value.buffer = (char *) &sql_args[i].int_value;
                bind.value.buffer_size = sizeof(sql_args[i].int_value);
                bind.value.length = &bind.value.buffer_size;
                break;
            case A_VAL64: case A_UVAL64:
                bind.value.type = A_VAL64;
                bind.value.buffer = (char *) &sql_args[i].wideint_value;
                bind.value.buffer_size = sizeof(sql_args[i].wideint_value);
                bind.value.length = &bind.value.buffer_size;
                break;
            case A_DOUBLE: case A_FLOAT:
                bind.value.type = A_DOUBLE;
                bind.value.buffer = (char *) &sql_args[i].double_value;
                bind.value.buffer_size = sizeof(sql_args[i].double_value);
                bind.value.length = &bind.value.buffer_size;
                break;
            default: {
                // Strings and binaries (byte arrays)
            }
        }

        Tcl_Obj * arg_val;
        if ( bind.direction == DD_INPUT ) {
            arg_val = argv[i];
        } else {
            if ( bind.direction == DD_INPUT_OUTPUT ) {
                arg_val = Tcl_ObjGetVar2(interp, argv[i], NULL, TCL_LEAVE_ERR_MSG);
                if ( arg_val == NULL ) {
                    return TCL_ERROR;
                }
            } else {
                arg_val = Tcl_ObjGetVar2(interp, argv[i], NULL, 0);
                if ( arg_val == NULL ) {
                    arg_val = Tcl_ObjSetVar2(interp, argv[i], NULL, Tcl_NewObj(), TCL_LEAVE_ERR_MSG);
                }
            }
            if ( Tcl_IsShared(arg_val) ) {
                arg_val = Tcl_ObjSetVar2(interp, argv[i], NULL, Tcl_DuplicateObj(arg_val), TCL_LEAVE_ERR_MSG);
            }
        }

        is_null[i] = ( arg_val->bytes != NULL && arg_val->length == 0 );
        if ( is_null[i] ) {
            bind.value.is_null = &is_null[i];
        } else if ( bind.direction == DD_INPUT || bind.direction == DD_INPUT_OUTPUT ) {
            // Load primities into local buffers
            switch ( bind.value.type ) {
                case A_VAL32: case A_UVAL32: case A_VAL16: case A_UVAL16: case A_VAL8: case A_UVAL8: {
                    int res = (
                        info.native_type == DT_BOOLEAN
                            ? Tcl_GetBooleanFromObj(interp, arg_val, &sql_args[i].int_value)
                            : Tcl_GetIntFromObj(interp, arg_val, &sql_args[i].int_value)
                    );
                    if ( res != TCL_OK ) {
                        return TCL_ERROR;
                    }
                    break;
                }
                case A_VAL64: case A_UVAL64: {
                    if ( Tcl_GetWideIntFromObj(interp, arg_val, &sql_args[i].wideint_value) != TCL_OK ) {
                        return TCL_ERROR;
                    }
                    break;
                }
                case A_DOUBLE: case A_FLOAT: {
                    if ( Tcl_GetDoubleFromObj(interp, arg_val, &sql_args[i].double_value) != TCL_OK ) {
                        return TCL_ERROR;
                    }
                    break;
                }
                default: {
                    // Strings and binaries (byte arrays)
                }
            }
        }

        // Strings and byte arrays
        if ( bind.direction == DD_INPUT || bind.direction == DD_INPUT_OUTPUT ) {
            int len;
            switch ( bind.value.type ) {
                case A_STRING:
                    bind.value.buffer = Tcl_GetStringFromObj(arg_val, &len);
                    sql_args[i].data_length = len;
                    bind.value.length = &sql_args[i].data_length;
                    break;
                case A_BINARY:
                    bind.value.buffer = (char *) Tcl_GetByteArrayFromObj(arg_val, &len);
                    sql_args[i].data_length = len;
                    bind.value.length = &sql_args[i].data_length;
                    break;
                default: {
                    // primitives
                }
            }
        }

        if ( bind.direction == DD_OUTPUT || bind.direction == DD_INPUT_OUTPUT ) {
            // Make sure Strings and byte arrays have enough space for output
            if ( bind.value.type == A_STRING ) {
                // For NVARCHAR DBCAPI sets bind's "buffer size" (and info's "max size") to the number of characters.
                int output_buffer_size = bind.value.buffer_size * BYTES_PER_CODEPOINT;
                int len;
                bind.value.buffer = Tcl_GetStringFromObj(arg_val, &len);
                if ( len < output_buffer_size ) {
                    Tcl_SetObjLength(arg_val, output_buffer_size);
                    bind.value.buffer = (char*) Tcl_GetString(arg_val);
                    // account for terminating '\0'
                    bind.value.buffer_size = output_buffer_size + 1;
                } else {
                    bind.value.buffer_size = len + 1;
                }
                bind.value.length = &sql_args[i].data_length;

            } else if ( bind.value.type == A_BINARY ) {
                int len;
                bind.value.buffer = (char *) Tcl_GetByteArrayFromObj(arg_val, &len);
                if ( len < bind.value.buffer_size ) {
                    bind.value.buffer = (char*) Tcl_SetByteArrayLength(arg_val, bind.value.buffer_size);
                } else {
                    bind.value.buffer_size = len;
                }
                bind.value.length = &sql_args[i].data_length;
                Tcl_InvalidateStringRep(arg_val);
            }
        }

        BIND_PARAM(stmt_state_ptr, interp, i, &bind);
    }

    return TCL_OK;
}

/**
 * Sends IN LOB argument data that were provided as a readable channel.
 */
static int
SendDataFromChannel (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int arg_idx, dbcapi_data_type data_type, Tcl_Channel input)
{
    if ( data_type == A_STRING ) {
        Tcl_Obj * buff = Tcl_NewObj();
        int len = 32768;
        Tcl_SetObjLength(buff, len);
        do {
            len = Tcl_ReadChars(input, buff, 32768, 0);
            int byte_len;
            char * data = Tcl_GetStringFromObj(buff, &byte_len);
            if ( !dbcapi.send_param_data(stmt_state_ptr->stmt, arg_idx, data, byte_len) ) {
                char num[12];
                SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot send data for LOB argument [", itoa(arg_idx, num, 10), "]", NULL);
                Tcl_DecrRefCount(buff);
                return TCL_ERROR;
            }
        } while ( len == 32768 );
        Tcl_DecrRefCount(buff);
        if ( !dbcapi.finish_param_data(stmt_state_ptr->stmt, arg_idx) ) {
            char num[12];
            SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot finish sending data for LOB argument [", itoa(arg_idx, num, 10), "]", NULL);
            return TCL_ERROR;
        }
    } else if (data_type == A_BINARY ) {
        Tcl_Obj * buff = Tcl_NewObj();
        int len = 32768;
        Tcl_SetByteArrayLength(buff, len);
        do {
            len = Tcl_ReadChars(input, buff, 32768, 0);
            int byte_len;
            unsigned char * data = Tcl_GetByteArrayFromObj(buff, &byte_len);
            if ( !dbcapi.send_param_data(stmt_state_ptr->stmt, arg_idx, (char *) data, byte_len) ) {
                char num[12];
                SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot send data for LOB argument [", itoa(arg_idx, num, 10), "]", NULL);
                Tcl_DecrRefCount(buff);
                return TCL_ERROR;
            }
        } while ( len == 32768 );
        Tcl_DecrRefCount(buff);
        if ( !dbcapi.finish_param_data(stmt_state_ptr->stmt, arg_idx) ) {
            char num[12];
            SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot finish sending data for LOB argument [", itoa(arg_idx, num, 10), "]", NULL);
            return TCL_ERROR;
        }
    } else {
        char num[12];
        Tcl_AppendResult(interp, "DBCAPI reported that LOB parameter [", itoa(arg_idx, num, 10), "] is neither a string, nor it is a binary", NULL);
        return TCL_ERROR;
    }
    return TCL_OK;
}

/**
 * Sends IN LOB argument data that were provided as a TCL object.
 */
static int
SendDataFromObject (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int arg_idx, dbcapi_data_type data_type, Tcl_Obj * input)
{
    int len;
    char * data;

    if ( data_type == A_STRING ) {
        data = Tcl_GetStringFromObj(input, &len);
    } else if (data_type == A_BINARY ) {
        data = (char *) Tcl_GetByteArrayFromObj(input, &len);
    } else {
        char num[12];
        Tcl_AppendResult(interp, "DBCAPI reported that LOB parameter [", itoa(arg_idx, num, 10), "] is neither a string, nor it is a binary", NULL);
        return TCL_ERROR;
    }

    if ( !dbcapi.send_param_data(stmt_state_ptr->stmt, arg_idx, data, len) ) {
        char num[12];
        SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot send data for LOB argument [", itoa(arg_idx, num, 10), "]", NULL);
        return TCL_ERROR;
    }
    if ( !dbcapi.finish_param_data(stmt_state_ptr->stmt, arg_idx) ) {
        char num[12];
        SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot finish sending data for LOB argument [", itoa(arg_idx, num, 10), "]", NULL);
        return TCL_ERROR;
    }
    return TCL_OK;
}

/**
 * Sends IN and INOUT statement LOB arguments to the database.
 *
 * \note Data for an IN LOB can be provided as an objects (those are handled elsewhere) or a readable channel (these are handled here).
 *       Data for INOUT LOBs can be provided (indirectly via a variable) as an object or a read-write channel (IN data will be read
 *       from that channel and later OUT data will be written to the same channel) or as a list with 2 elements. Element at index 0
 *       provides data for an IN part of the execution (object, variable or a readable channel) and element at index 0 indicates where
 *       OUT data should be saved (variable or a writable channel)
 */
static int
SendStmtInput (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int argc, Tcl_Obj * const argv[])
{
    for ( int i = 0; i < argc; i++ ) {
        dbcapi_bind_param_info info;
        if ( !dbcapi.get_bind_param_info(stmt_state_ptr->stmt, i, &info) ) {
            char num[12];
            SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve information about SQL parameter [", itoa(i, num, 10), "]", NULL);
            return TCL_ERROR;
        }
        if ( info.direction == DD_OUTPUT || info.input_value.buffer_size != 0 ) {
            continue;
        }

        if ( info.direction == DD_INPUT ) {
            // Can only be a channel at this point. It also has been validated during binding.
            const char * name = Tcl_GetString(argv[i]);
            Tcl_Channel input = Tcl_GetChannel(interp, name, NULL);
            if ( SendDataFromChannel(stmt_state_ptr, interp, i, info.input_value.type, input) != TCL_OK ) {
                return TCL_ERROR;
            }
            continue;
        }

        // DD_INPUT_OUTPUT
        // This case is a bit complicated as the caller is allowed to specify input and output separately (as a list).
        int len;
        if ( Tcl_ListObjLength(interp, argv[i], &len) != TCL_OK ) {
            return TCL_ERROR;
        }
        if ( len != 1 && len != 2 ) {
            SetNumDiffErrorResult(interp, "Argument [", i, "] should have 1 or 2 values in it but ", len, " were found.");
            return TCL_ERROR;
        }
        Tcl_Obj * input;
        if ( Tcl_ListObjIndex(interp, argv[i], 0, &input) != TCL_OK ) {
            return TCL_ERROR;
        }

        if ( len == 1 ) {
            // This can be either a read-write channel or a variable...
            const char * name = Tcl_GetString(input);
            int mode;
            Tcl_Channel input_channel = Tcl_GetChannel(interp, name, &mode);
            if ( input_channel != NULL ) {
                if ( (mode & TCL_READABLE) == 0 || (mode & TCL_WRITABLE) == 0 ) {
                    Tcl_AppendResult(interp, "Channel ", name, " must be open for reading and writing", NULL);
                    return TCL_ERROR;
                }
                if ( SendDataFromChannel(stmt_state_ptr, interp, i, info.input_value.type, input_channel) != TCL_OK ) {
                    return TCL_ERROR;
                }

            } else {
                // A variable then
                Tcl_ResetResult(interp);

                Tcl_Obj * input_value = Tcl_ObjGetVar2(interp, input, NULL, TCL_LEAVE_ERR_MSG);
                if ( input_value == NULL ) {
                    return TCL_ERROR;
                }
                if ( SendDataFromObject(stmt_state_ptr, interp, i, info.input_value.type, input_value) != TCL_OK ) {
                    return TCL_ERROR;
                }
            }
        } else {
            // The first element can be a readable channel or an object
            // We would rather avoid conversion (in case parameter is for a binary LOB and the "input" is a binary object),
            // so we'll peek into object to see if what it has might be a channel name.
            if ( input->bytes != NULL && 0 < input->length && input->length <= 32 ) {
                const char * name = Tcl_GetString(input);
                int mode;
                Tcl_Channel input_channel = Tcl_GetChannel(interp, name, &mode);
                if ( input_channel != NULL ) {
                    if ( (mode & TCL_READABLE) == 0 ) {
                        Tcl_AppendResult(interp, "Channel ", name, " must be open for reading", NULL);
                        return TCL_ERROR;
                    }
                    if ( SendDataFromChannel(stmt_state_ptr, interp, i, info.input_value.type, input_channel) != TCL_OK ) {
                        return TCL_ERROR;
                    }
                    continue;
                }
            }
            // If it is not a channel, then this is an object and we'll send its content
            if ( SendDataFromObject(stmt_state_ptr, interp, i, info.input_value.type, input) != TCL_OK ) {
                return TCL_ERROR;
            }
        }
    }
    return TCL_OK;
}

/**
 * Saves LOB data into a writable channel.
 */
static int
SaveDataToChannel (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int arg_idx, dbcapi_data_type data_type, Tcl_Channel output)
{
    Tcl_Obj * buff = Tcl_NewObj();
    Tcl_IncrRefCount(buff);
    size_t offset = 0;
    int res = TCL_OK;
    do {
        char * data;
        int buff_size;
        if ( data_type == A_STRING ) {
            Tcl_SetObjLength(buff, 32768);
            data = Tcl_GetStringFromObj(buff, &buff_size);
        } else {
            Tcl_SetByteArrayLength(buff, 32768);
            data = (char *) Tcl_GetByteArrayFromObj(buff, &buff_size);
        }

        int len = dbcapi.get_param_data(stmt_state_ptr->stmt, arg_idx, offset, data, buff_size);
        if ( len < 0 ) {
            SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve LOB data from [", arg_idx, "]", NULL);
            res = TCL_ERROR;
            break;
        }
        if ( len == 0 ) {
            break;
        }
        offset += len;

        if ( data_type == A_STRING ) {
            Tcl_SetObjLength(buff, len);
        } else {
            Tcl_SetByteArrayLength(buff, len);
        }

        if ( Tcl_WriteObj(output, buff) < 0 ) {
            res = TCL_ERROR;
        }

    } while ( res == TCL_OK );

    Tcl_DecrRefCount(buff);
    return res;
}

/**
 * Saves LOB data into a TCL object. This object is expected to be assigned to a variable.
 */
static int
SaveDataToObject (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int arg_idx, dbcapi_data_type data_type, Tcl_Obj * output)
{
    size_t offset = 0;
    int read_len;
    do {
        char * data;
        int data_size;
        int buff_size;
        if ( data_type == A_STRING ) {
            data = Tcl_GetStringFromObj(output, &data_size);
            Tcl_SetObjLength(output, data_size + 32768);
            data = Tcl_GetStringFromObj(output, &buff_size);
        } else {
            data = (char *) Tcl_GetByteArrayFromObj(output, &data_size);
            Tcl_SetByteArrayLength(output, data_size + 32768);
            data = (char *) Tcl_GetByteArrayFromObj(output, &buff_size);
        }
        read_len = dbcapi.get_param_data(stmt_state_ptr->stmt, arg_idx, offset, data + data_size, buff_size - data_size);
        if ( read_len < 0 ) {
            SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve data from parameter [", arg_idx, "]", NULL);
            return TCL_ERROR;
        }
        if ( data_type == A_STRING ) {
            Tcl_SetObjLength(output, data_size + read_len);
        } else {
            Tcl_SetByteArrayLength(output, data_size + read_len);
        }
        offset += read_len;
    } while ( read_len > 0 );
    return TCL_OK;
}

/**
 * Saves data returned for OUT or INOUT bound variables.
 *
 * \note OUT LOB data can be saved into a provided variable or into a channel. The latter must be writable.
 */
static int
SaveStmtOutput (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int argc, Tcl_Obj * const argv[], dbcapi_bool is_null[], PrimitiveSqlValue sql_args[])
{
    for ( int i = 0; i < argc; i++ ) {
        dbcapi_bind_param_info info;
        if ( !dbcapi.get_bind_param_info(stmt_state_ptr->stmt, i, &info) ) {
            char num[12];
            SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve information about SQL argument [", itoa(i, num, 10), "]", NULL);
            return TCL_ERROR;
        }

        if ( info.direction == DD_INPUT ) {
            continue;
        }

        if ( info.output_value.buffer_size == 0 ) {
            // An output from a LOB

            Tcl_Obj * output;
            if ( info.direction == DD_INPUT_OUTPUT ) {
                // Note that when the input was sent the argument had been converted into a list
                if ( Tcl_ListObjIndex(interp, argv[i], 1, &output) != TCL_OK ) {
                    return TCL_ERROR;
                }
            } else { // DD_OUTPUT
                output = argv[i];
            }

            // The argument is either a writable channel or a variable
            const char * name = Tcl_GetString(output);
            int mode;
            Tcl_Channel output_channel = Tcl_GetChannel(interp, name, &mode);
            if ( output_channel != NULL ) {
                if ( (mode & TCL_WRITABLE) == 0 ) {
                    Tcl_AppendResult(interp, "Channel ", name, " must be open for writing", NULL);
                    return TCL_ERROR;
                }
                if ( SaveDataToChannel(stmt_state_ptr, interp, i, info.output_value.type, output_channel) != TCL_OK ) {
                    return TCL_ERROR;
                }
            } else {
                // a variable then
                Tcl_ResetResult(interp);

                Tcl_Obj * output_value = Tcl_ObjGetVar2(interp, output, NULL, 0);
                if ( output_value == NULL ) {
                    output_value = Tcl_ObjSetVar2(interp, output, NULL, Tcl_NewObj(), TCL_LEAVE_ERR_MSG);
                } else if ( Tcl_IsShared(output_value) ) {
                    output_value = Tcl_ObjSetVar2(interp, output, NULL, Tcl_DuplicateObj(output_value), TCL_LEAVE_ERR_MSG);
                }
                if ( output_value == NULL ) {
                    return TCL_ERROR;
                }
                if ( SaveDataToObject(stmt_state_ptr, interp, i, info.output_value.type, output_value) != TCL_OK ) {
                    return TCL_ERROR;
                }
            }

        } else {
            // Everything else (not a LOB) outputs into an object.
            // At this time the output has been already completed into the bound buffers.

            Tcl_Obj * output = Tcl_ObjGetVar2(interp, argv[i], NULL, TCL_LEAVE_ERR_MSG);
            if ( output == NULL ) {
                return TCL_ERROR;
            }
            switch ( info.output_value.type ) {
                case A_UVAL8:
                    if ( info.native_type == DT_BOOLEAN ) {
                        Tcl_SetBooleanObj(output, sql_args[i].int_value);
                    } else {
                        Tcl_SetIntObj(output, sql_args[i].int_value);
                    }
                    break;

                case A_VAL32: case A_UVAL32: case A_VAL16: case A_UVAL16: case A_VAL8:
                    Tcl_SetIntObj(output, sql_args[i].int_value);
                    break;

                case A_VAL64: case A_UVAL64:
                    Tcl_SetWideIntObj(output, sql_args[i].wideint_value);
                    break;

                case A_DOUBLE: case A_FLOAT:
                    Tcl_SetDoubleObj(output, sql_args[i].double_value);
                    break;

                case A_BINARY:
                    Tcl_SetByteArrayLength(output, sql_args[i].data_length);
                    break;

                case A_STRING:
                    Tcl_SetObjLength(output, sql_args[i].data_length);
                    break;

                default: {
                    // A_INVALID_TYPE
                }
            }
        }
    }
    return TCL_OK;
}

/**
 * Executes a prepared (or previsouly executed and thus prepared) statement.
 *
 * # Example
 *
 * \code{.tcl}
 * set stmt [$conn prepare "INSERT INTO employees (employee_id, first_name, last_name) VALUES (?, ?, ?)"]
 * $stmt execute 2 "Hasso" "Plattner"
 * \endcode
 */
static int
Stmt_Execute (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    dbcapi_bool         is_null[objc];
    PrimitiveSqlValue   sql_args[objc];

    if ( BindStmtArgs(stmt_state_ptr, interp, objc, objv, is_null, sql_args) != TCL_OK ) {
        return TCL_ERROR;
    }

    if ( !dbcapi.execute(stmt_state_ptr->stmt) ) {
        SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot execute SQL", NULL);
        return TCL_ERROR;
    }

    if ( SendStmtInput(stmt_state_ptr, interp, objc, objv) != TCL_OK ) {
        return TCL_ERROR;
    }

    if ( SaveStmtOutput(stmt_state_ptr, interp, objc, objv, is_null, sql_args) != TCL_OK ) {
        return TCL_ERROR;
    }

    return TCL_OK;
}

/**
 * Sets TCL (integer) result or TCL error depending on the value returned by a DBCAPI function.
 */
static int
StmtSetIntResult (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int result)
{
    if ( result < 0 ) {
        SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve requested number", NULL);
        return TCL_ERROR;
    }
    Tcl_SetObjResult(interp, Tcl_NewIntObj(result));
    return TCL_OK;
}

/**
 * Returns information about a column in the result set.
 *
 * The column info is returned as a dictionary with the following keys:
 *  - `owner`     - The name of the owner
 *  - `table`     - The name of the table
 *  - `name`      - The alias or the name of the column
 *  - `type`      - The native type of the column in the database
 *  - `size`      - The maximum size a data value in this column can take
 *  - `nullable`  - Indicates whether a value in the column can be null
 *  - `precision` - The precision
 *  - `scale`     - The scale
 *
 * # Example
 *
 * \code{.tcl}
 * set col_num 0
 * set col_info [$stmt get -columninfo $col_num]
 * \endcode
 */
static int
Stmt_ColumnInfo (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc != 1 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "get -columninfo column_number");
        return TCL_ERROR;
    }

    int col_num;
    if ( Tcl_GetIntFromObj(interp, objv[0], &col_num) != TCL_OK ) {
        return TCL_ERROR;
    }

    dbcapi_column_info info;
    if ( !dbcapi.get_column_info(stmt_state_ptr->stmt, col_num, &info) ) {
        char num[12];
        SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve column [", itoa(col_num, num, 10), "] info", NULL);
        return TCL_ERROR;
    }

    Hdbtcl_Literals * lit = &stmt_state_ptr->conn_state_ptr->hdbtcl_state_ptr->literals;

    Tcl_Obj * res = Tcl_NewDictObj();
    if ( Tcl_DictObjPut(interp, res, lit->owner, Tcl_NewStringObj(info.owner_name, -1)) != TCL_OK ) {
        goto Error_Exit;
    }
    if ( Tcl_DictObjPut(interp, res, lit->table, Tcl_NewStringObj(info.table_name, -1)) != TCL_OK ) {
        goto Error_Exit;
    }
    if ( Tcl_DictObjPut(interp, res, lit->name, Tcl_NewStringObj(info.column_name, -1)) != TCL_OK ) {
        goto Error_Exit;
    }
    if ( Tcl_DictObjPut(interp, res, lit->type, GetTypeName(lit, info.native_type)) != TCL_OK ) {
        goto Error_Exit;
    }
    Tcl_Obj * value = (info.max_size <= INT_MAX ? Tcl_NewIntObj(info.max_size) : Tcl_NewWideIntObj(info.max_size) );
    if ( Tcl_DictObjPut(interp, res, lit->size, value) != TCL_OK ) {
        goto Error_Exit;
    }
    if ( Tcl_DictObjPut(interp, res, lit->nullable, Tcl_NewBooleanObj(info.nullable)) != TCL_OK ) {
        goto Error_Exit;
    }
    if ( Tcl_DictObjPut(interp, res, lit->precision, Tcl_NewIntObj(info.precision)) != TCL_OK ) {
        goto Error_Exit;
    }
    if ( Tcl_DictObjPut(interp, res, lit->scale, Tcl_NewIntObj(info.scale)) != TCL_OK ) {
        goto Error_Exit;
    }

    Tcl_SetObjResult(interp, res);
    return TCL_OK;

Error_Exit:
    Tcl_DecrRefCount(res);
    return TCL_ERROR;
}

/**
 * Retrieves statement column names.
 *
 * # Example
 *
 * \code{.tcl}
 * set stmt [$conn execute "SELECT * FROM objects WHERE schema_name = ?" "SYS"]
 * set column_names [$stmt get -columnnames]
 * while { [$stmt fetch row] } {
 *     foreach name $column_names value $row {
 *         puts [format "%20s: %s" $name $value]
 *     }
 * }
 * \endcode
 */
static int
Stmt_ColumnNames (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc > 0 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "get -columnnames");
        return TCL_ERROR;
    }

    int num_cols = dbcapi.num_cols(stmt_state_ptr->stmt);
    Tcl_Obj * names[num_cols];
    for ( int col = 0; col < num_cols; ++col ) {
        dbcapi_column_info info;
        if ( !dbcapi.get_column_info(stmt_state_ptr->stmt, col, &info) ) {
            char num[12];
            SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve column [", itoa(col, num, 10), "] info", NULL);
            return TCL_ERROR;
        }
        names[col] = Tcl_NewStringObj(info.column_name, -1);
    }

    Tcl_SetObjResult(interp, Tcl_NewListObj(num_cols, names));
    return TCL_OK;
}

/**
 * Retrieve a print line printed during execution of a stored procedure.
 *
 * The initial call after execution of the stored procedure will retrieve the first line printed.
 * If the print line is successfully retrieved, it is removed from the queue and subsequent calls
 * will retrieve following print lines (if available).
 *
 * # Example
 *
 * \code{.sql}
 * CREATE PROCEDURE TEST_PROC AS
 * BEGIN
 *     USING SQLSCRIPT_PRINT AS PRINT;
 *     SELECT 1 FROM DUMMY;
 *     PRINT:PRINT_LINE('Hello, World!');
 *     SELECT 2 FROM DUMMY;
 *     PRINT:PRINT_LINE('Hello World, Again!');
 * END
 * \endcode
 *
 * \code{.tcl}
 * set stmt [$conn prepare "CALL test_proc"]
 * $stmt execute
 * while { [$stmt get -printline line] } {
 *     puts $line
 * }
 * \endcode
 *
 */
static int
StmtGetPrintLine (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc != 1 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "get -printline varname");
        return TCL_ERROR;
    }
    Tcl_Obj * line = Tcl_NewObj();
    int len;
    char * buffer = Tcl_GetStringFromObj(line, &len);

    bool zero_terminate_line = false;
    int host_type = 4; // SQLDBC_HOSTTYPE_UTF8
    size_t text_size;
    dbcapi_retcode res = dbcapi.get_print_line(stmt_state_ptr->stmt, host_type, buffer, &text_size, len, zero_terminate_line);
    if ( res == DBCAPI_NO_DATA_FOUND ) {
        Tcl_SetObjResult(interp, Tcl_NewBooleanObj(0));

    } else {
        if ( res == DBCAPI_DATA_TRUNC ) {
            Tcl_SetObjLength(line, text_size);
            buffer = Tcl_GetStringFromObj(line, &len);
            res = dbcapi.get_print_line(stmt_state_ptr->stmt, host_type, buffer, &text_size, len, zero_terminate_line);
            Tcl_SetObjLength(line, text_size);
        }
        if ( Tcl_ObjSetVar2(interp, objv[0], NULL, line, TCL_LEAVE_ERR_MSG) != NULL ) {
            Tcl_SetObjResult(interp, Tcl_NewBooleanObj(1));
        }
    }
    return TCL_OK;
}

/**
 * Statement "get" subcommand options multiplexor.
 */
static int
Stmt_Get (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc < 1 || 2 < objc ) {
        Tcl_WrongNumArgs(interp, objc, objv, "get -option ?attr?");
        return TCL_ERROR;
    }

    static const char * const options[] = {
        "-colinfo", "-columninfo", "-colnames", "-columnnames", "-numaffectedrows", "-numcols", "-numcolumns", "-numrows", "-printline", NULL
    };
    enum {
        COL_INFO, COLUMN_INFO, COL_NAMES, COLUMN_NAMES, NUM_AFFECTED_ROWS, NUM_COLS, NUM_COLUMNS, NUM_ROWS, PRINT_LINE
    } option;

    if ( Tcl_GetIndexFromObj(interp, objv[0], options, "option", 0, (int *) &option) != TCL_OK ) {
        return TCL_ERROR;
    }
    switch ( option ) {
        case COLUMN_INFO: case COL_INFO:
            return Stmt_ColumnInfo  (stmt_state_ptr, interp, objc - 1, objv + 1);
        case COLUMN_NAMES: case COL_NAMES:
            return Stmt_ColumnNames (stmt_state_ptr, interp, objc - 1, objv + 1);
        case NUM_AFFECTED_ROWS:
            return StmtSetIntResult(stmt_state_ptr, interp, dbcapi.affected_rows(stmt_state_ptr->stmt));
        case NUM_COLS: case NUM_COLUMNS:
            return StmtSetIntResult(stmt_state_ptr, interp, dbcapi.num_cols(stmt_state_ptr->stmt));
        case NUM_ROWS:
            return StmtSetIntResult(stmt_state_ptr, interp, dbcapi.num_rows(stmt_state_ptr->stmt));
        case PRINT_LINE:
            return StmtGetPrintLine(stmt_state_ptr, interp, objc - 1, objv + 1);
    }
    return TCL_OK;
}

/**
 * Fetches LOB data piece by piece and feeds pieces to the "LOB read command"
 */
static int
FetchLobColumn (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, dbcapi_column_info info[], int col, Tcl_Obj * lob_read_cmd, Tcl_Obj * lob_read_init_state)
{
    int res = TCL_OK;
    if ( lob_read_init_state == NULL ) {
        lob_read_init_state = Tcl_NewObj();
    }
    const char * col_name = ( info[col].name != NULL && info[col].name[0] != '\0' ? info[col].name : info[col].column_name );
    // cmd $cmd_data $lob_data $col_num $col_name $table_name $table_owner
    Tcl_Obj * lob_read_objv[7];
    lob_read_objv[0] = lob_read_cmd;
    lob_read_objv[1] = lob_read_init_state;
    lob_read_objv[2] = Tcl_NewObj();
    lob_read_objv[3] = Tcl_NewIntObj(col);
    lob_read_objv[4] = Tcl_NewStringObj(col_name, -1);
    lob_read_objv[5] = Tcl_NewStringObj(info[col].table_name, -1);
    lob_read_objv[6] = Tcl_NewStringObj(info[col].owner_name, -1);
    for ( int i = 0; i < 7; ++i ) {
        Tcl_IncrRefCount(lob_read_objv[i]);
    }
    Tcl_Obj * buff = lob_read_objv[2];
    int read_len;
    size_t offset = 0;
    do {
        char * data;
        int buff_size;

        if ( info[col].type == A_STRING ) {
            Tcl_SetObjLength(buff, 32768);
            data = Tcl_GetStringFromObj(buff, &buff_size);
        } else {
            Tcl_SetByteArrayLength(buff, 32768);
            data = (char *) Tcl_GetByteArrayFromObj(buff, &buff_size);
        }

        read_len = dbcapi.get_data(stmt_state_ptr->stmt, col, offset, data, buff_size);
        if ( read_len < 0 ) {
            SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve data from LOB column ", col_name, NULL);
            res = TCL_ERROR;
            break;
        }
        if ( info[col].type == A_STRING ) {
            Tcl_SetObjLength(buff, read_len);
        } else {
            Tcl_SetByteArrayLength(buff, read_len);
        }
        offset += read_len;

        res = Tcl_EvalObjv(interp, 7, lob_read_objv, 0);
        if ( res != TCL_OK ) {
            break;
        }
        // tranfer returned result into cmd_data
        Tcl_DecrRefCount(lob_read_objv[1]);
        lob_read_objv[1] = Tcl_GetObjResult(interp);
        Tcl_IncrRefCount(lob_read_objv[1]);

    } while ( read_len > 0 );

    for ( int i = 0; i < 7; ++i ) {
        Tcl_DecrRefCount(lob_read_objv[i]);
    }
    return res;
}

/**
 * Fetches the next row from the result set and saves it into the specified variable.
 *
 * # Example
 *
 * \code{.tcl}
 * while { [$stmt fetch row] } {
 *     # row is a list of column values
 * }
 * \endcode
 *
 * If one of the columns is a LOB, fetch will try, memory permitting, to fetch the entire
 * LOB into a string or a binary, depending on the column type. While this works for
 * some use cases, it might not be feasible for very large LOBs. Large LOBs can be fetched
 * piece by piece via custom LOB read callbacks:
 *
 * # Example
 *
 * \code{.tcl}
 * proc read_lob { read_state lob_chunk col_num col_name table_name table_owner } {
 *     # Where:
 *     #   read_state  - current reading state. Can be anything. Initial value is {}.
 *     #   lob_chunk   - current piece of LOB data. It'll be a string or a binary depending
 *     #                 on the LOB type.
 *     #   col_num     - LOB column number in the result set row.
 *     #   col_name    - LOB column name
 *     #   table_name  - table name from which LOB is retrieved.
 *     #   table_owner - LOB table schema.
 *
 *     # This procedure returns modified state to be passed to the call
 *     # that reads the next LOB piece.
 *     return $read_state
 * }
 *
 * while { [$stmt fetch row -lobreadcommand read_lob] } {
 *     # non LOB columns will be retrieved by fetch
 *     # LOB columns in the returned row will contain the final "read state"
 * }
 * \endcode
 *
 * While the default initial read state is {} it can be set to a different value via
 * -lobreadinitialstate.
 *
 * # Example
 *
 * \code{.tcl}
 * set name_prefix "temp_lob_file"
 * while { [$stmt fetch row -lobreadcommand read_lob_info_file -lobreadinitialstate $name_prefix] } {
 *     # process columns from the row
 * }
 * \endcode
 */
static int
Stmt_Fetch (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc != 1 && objc != 3 && objc != 5 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "fetch row_var ?-lobreadcommand cmd_name ?-lobreadinitialstate init_state??");
        return TCL_ERROR;
    }
    Tcl_Obj * lob_read_cmd = NULL;
    Tcl_Obj * lob_read_init_state = NULL;
    if ( objc >= 3 ) {
        static const char * const options[] = { "-lobreadcmd", "-lobreadcommand", "-lobreadinit", "-lobreadinitialstate", NULL };
        enum { LOBREADCMD, LOBREADCOMMAND, LOBREADINIT, LOBREADINITIALSTATE } option;
        for ( int i = 1; i < objc; i+=2 ) {
            if ( Tcl_GetIndexFromObj(interp, objv[i], options, "option", 0, (int *) &option) != TCL_OK ) {
                return TCL_ERROR;
            }
            switch ( option ) {
                case LOBREADCMD: case LOBREADCOMMAND: {
                    lob_read_cmd = objv[i+1];
                    if ( Tcl_GetCommandFromObj(interp, lob_read_cmd) == NULL ) {
                        const char * cmd_name = Tcl_GetString(lob_read_cmd);
                        Tcl_AppendResult(interp, "command ", cmd_name, " does not exist", NULL);
                        return TCL_ERROR;
                    }
                    break;
                }
                case LOBREADINIT: case LOBREADINITIALSTATE: {
                    lob_read_init_state = objv[i+1];
                    break;
                }
            }
        }
    }

    int num_cols = dbcapi.num_cols(stmt_state_ptr->stmt);
    if ( num_cols < 0 ) {
        SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve result set info (column count)", NULL);
        return TCL_ERROR;
    }
    if ( num_cols == 0 ) {
        Tcl_AppendResult(interp, "This statement did not return any rows");
        return TCL_ERROR;
    }
    dbcapi_column_info info[num_cols];
    for ( int col = 0; col < num_cols; ++col ) {
        if ( !dbcapi.get_column_info(stmt_state_ptr->stmt, col, &info[col]) ) {
            char num[12];
            SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve column [", itoa(col, num, 10), "] info", NULL);
            return TCL_ERROR;
        }
    }

    Tcl_Obj * row = Tcl_ObjSetVar2(interp, objv[0], NULL, Tcl_NewListObj(0, NULL), TCL_LEAVE_ERR_MSG);
    if ( row == NULL ) {
        return TCL_ERROR;
    }

    int fetched = dbcapi.fetch_next(stmt_state_ptr->stmt);
    if ( fetched ) {
        for ( int col = 0; col < num_cols; ++col ) {
            Tcl_Obj * col_val;
            if ( info[col].max_size == INT32_MAX && lob_read_cmd != NULL ) {
                if ( FetchLobColumn(stmt_state_ptr, interp, info, col, lob_read_cmd, lob_read_init_state) != TCL_OK ) {
                    return TCL_ERROR;
                }
                col_val = Tcl_GetObjResult(interp);
            } else {
                dbcapi_data_value value;
                if ( !dbcapi.get_column(stmt_state_ptr->stmt, col, &value) ) {
                    char num[12];
                    SetErrorResult(interp, stmt_state_ptr->conn_state_ptr->conn, "Cannot retrieve column [", itoa(col, num, 10), "] data", NULL);
                    return TCL_ERROR;
                }
                col_val = Tcl_NewObj();
                if ( !*value.is_null ) {
                    switch ( value.type ) {
                        case A_UVAL8:
                            if ( info[col].native_type == DT_BOOLEAN ) {
                                Tcl_SetBooleanObj(col_val, *(uint8_t *)value.buffer);
                            } else {
                                Tcl_SetIntObj(col_val, *(uint8_t *)value.buffer);
                            }
                            break;
                        case A_VAL8:
                            Tcl_SetIntObj(col_val, *(int8_t *)value.buffer);
                            break;
                        case A_UVAL16:
                            Tcl_SetIntObj(col_val, *(uint16_t *)value.buffer);
                            break;
                        case A_VAL16:
                            Tcl_SetIntObj(col_val, *(int16_t *)value.buffer);
                            break;
                        case A_UVAL32:
                            Tcl_SetIntObj(col_val, *(uint32_t *)value.buffer);
                        case A_VAL32:
                            Tcl_SetIntObj(col_val, *(int32_t *)value.buffer);
                            break;
                        case A_UVAL64:
                            Tcl_SetWideIntObj(col_val, *(uint64_t *)value.buffer);
                            break;
                        case A_VAL64:
                            Tcl_SetWideIntObj(col_val, *(int64_t *)value.buffer);
                            break;
                        case A_DOUBLE:
                            Tcl_SetDoubleObj(col_val, *(double *)value.buffer);
                            break;
                        case A_FLOAT:
                            Tcl_SetDoubleObj(col_val, (double)*(float *)value.buffer);
                            break;
                        case A_BINARY:
                            Tcl_SetByteArrayObj(col_val, (unsigned char*) value.buffer, *value.length);
                            break;
                        case A_STRING:
                            Tcl_SetStringObj(col_val, value.buffer, *value.length);
                            break;
                        default: {
                            // A_INVALID_TYPE
                        }
                    }
                }
            }
            if ( Tcl_ListObjAppendElement(interp, row, col_val) != TCL_OK ) {
                return TCL_ERROR;
            }
        }
    }

    Tcl_SetObjResult(interp, Tcl_NewBooleanObj(fetched));
    return TCL_OK;
}

/**
 * Advances to the next result set in a multiple result set query.
 *
 * If a query (such as a call to a stored procedure) returns multiple result sets,
 * then "nextresult" advances from the current result set to the next.
 *
 * "nextresult" returns true if the statement successfully advances to the next
 * result set, false otherwise.
 *
 * # Example
 *
 * \code{.tcl}
 * set stmt [$conn execute "call multiple_results_procedure()"]
 * while { 1 } {
 *     while { [$stmt fetch row] } {
 *         # process columns from the fetched row
 *     }
 *     if { ![$stmt nextresult] } {
 *         break
 *     }
 * }
 * \endcode
 */
static int
Stmt_NextResult (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc != 0 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "nextresult");
        return TCL_ERROR;
    }
    dbcapi_bool has_advanced = dbcapi.get_next_result(stmt_state_ptr->stmt);
    Tcl_SetObjResult(interp, Tcl_NewBooleanObj(has_advanced));
    return TCL_OK;
}

/**
 * Statement subcommands multiplexor.
 */
static int
Stmt_Cmd (Stmt_State * stmt_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc < 2 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "method ?option...?");
        return TCL_ERROR;
    }

    static const char * const methods[] = {
        "close", "execute", "fetch", "get", "nextresult", NULL
    };
    enum {
        CLOSE, EXECUTE, FETCH, GET, NEXT_RESULT
    } method;

    if ( Tcl_GetIndexFromObj(interp, objv[1], methods, "method", 0, (int *) &method) != TCL_OK ) {
        return TCL_ERROR;
    }
    switch ( method ) {
        case CLOSE:
            return Stmt_Close       (stmt_state_ptr, interp, objc - 2, objv + 2);
        case EXECUTE:
            return Stmt_Execute     (stmt_state_ptr, interp, objc - 2, objv + 2);
        case FETCH:
            return Stmt_Fetch       (stmt_state_ptr, interp, objc - 2, objv + 2);
        case GET:
            return Stmt_Get         (stmt_state_ptr, interp, objc - 2, objv + 2);
        case NEXT_RESULT:
            return Stmt_NextResult  (stmt_state_ptr, interp, objc - 2, objv + 2);
    }
    return TCL_OK;
}

/**
 * Closes the current database connection.
 *
 * All currently opened cursors (statements) are also closed. Saved connection and statement
 * become invalid as corresponding command are deleted.
 *
 * # Example
 *
 * \code{.tcl}
 * $conn close
 * \endcode
 *
 * \note It is not necessary to close a connection explicitly. `hdbtcl` will close it automatically
 * when all references to the connection are gone.
 */
static int
Conn_Close (Conn_State * conn_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( Tcl_DeleteCommandFromToken(interp, conn_state_ptr->conn_cmd) != TCL_OK ) {
        Tcl_SetResult(interp, "Cannot delete connection handle", TCL_STATIC);
        return TCL_ERROR;
    }
    return TCL_OK;
}

/**
 * Changes current connection configuration.
 *
 * Supported options:
 *  -autocommit
 *      Sets the AUTOCOMMIT mode to be on or off. The default AUTOCOMMIT mode is off, so
 *      calling the commit or rollback is required for each transaction. When the AUTOCOMMIT
 *      mode is set to on, all statements are committed after they execute. They cannot be
 *      rolled back.
 *  -isolation
 *      Sets the transaction isolation level. The default isolation level is "READ COMMITTED".
 *      Other two possible options are "REPEATABLE READ" and "SERIALIZABLE".
 *
 * # Example
 *
 * \code{.tcl}
 * $conn configure -autocommit true -isolation "REPEATABLE READ"
 * \endcode
 */
static int
Conn_Configure (Conn_State * conn_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc < 4 || objc % 2 != 0 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "configure -option value ?-option value...?");
        return TCL_ERROR;
    }

    static const char * const options[] = {
        "-autocommit", "-isolation",
        NULL
    };
    enum {
        AUTOCOMMIT, ISOLATION
    } option;

    for ( int i = 2; i < objc; i += 2 ) {
        if ( Tcl_GetIndexFromObj(interp, objv[i], options, "option", 0, (int *) &option) != TCL_OK ) {
            return TCL_ERROR;
        }
        switch ( option ) {
            case AUTOCOMMIT: {
                int autocommit;
                if ( Tcl_GetBooleanFromObj(interp, objv[i + 1], &autocommit) != TCL_OK ) {
                    return TCL_ERROR;
                }
                if ( !dbcapi.set_autocommit(conn_state_ptr->conn, autocommit) ) {
                    SetErrorResult(interp, conn_state_ptr->conn, "Cannot set autocommit", NULL);
                    return TCL_ERROR;
                }
                break;
            }
            case ISOLATION: {
                static const char * const levels[] = {
                    "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE",
                    NULL
                };
                int level;
                if ( Tcl_GetIndexFromObj(interp, objv[i + 1], levels, "isolation level", 0, &level) != TCL_OK ) {
                    return TCL_ERROR;
                }
                if ( !dbcapi.set_transaction_isolation(conn_state_ptr->conn, level + 1) ) {
                    SetErrorResult(interp, conn_state_ptr->conn, "Cannot set transaction isolation level", NULL);
                    return TCL_ERROR;
                }
                break;
            }
        }
    }
    return TCL_OK;
}

/**
 * Retrieves connection configration. Only -autocommit is supported.
 *
 * # Example
 *
 * \code{.tcl}
 * set is_autocommitting [$conn cget -autocommit]
 * \endcode
 */
static int
Conn_Cget (Conn_State * conn_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc != 3 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "cget -option");
        return TCL_ERROR;
    }

    static const char * const options[] = {
        "-autocommit",
        NULL
    };
    enum {
        AUTOCOMMIT
    } option;

    if ( Tcl_GetIndexFromObj(interp, objv[2], options, "option", 0, (int *) &option) != TCL_OK ) {
        return TCL_ERROR;
    }
    switch ( option ) {
        case AUTOCOMMIT: {
            dbcapi_bool autocommit;
            if ( !dbcapi.get_autocommit(conn_state_ptr->conn, &autocommit) ) {
                SetErrorResult(interp, conn_state_ptr->conn, "Cannot retrieve autocommit setting", NULL);
                return TCL_ERROR;
            }
            Tcl_SetObjResult(interp, Tcl_NewBooleanObj(autocommit));
            break;
        }
    }
    return TCL_OK;
}

/**
 * Sets or retrieves session-specific client information (name and value pair).
 *
 * \see https://help.sap.com/viewer/0eec0d68141541d1b07893a39944924e/2.0.04/en-US/e90fa1f0e06e4840aa3ee2278afae16b.html
 *
 * # Example
 *
 * \code{.tcl}
 * set current_app_name [$conn set APPLICATION]
 * $conn set APPLICATION tclapp
 * \endcode
 *
 * \note When setting client information this subcommand behaves like TCL's `set` command
 * and returns the value that was provided to it.
 */
static int
Conn_Set (Conn_State * conn_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc == 4 ) {

        char * var = Tcl_GetString(objv[2]);
        char * val = Tcl_GetString(objv[3]);
        if ( !dbcapi.set_clientinfo(conn_state_ptr->conn, var, val) ) {
            SetErrorResult(interp, conn_state_ptr->conn, "Cannot set session variable ", var, NULL);
            return TCL_ERROR;
        }
        Tcl_SetObjResult(interp, objv[3]);

    } else if ( objc == 3 ) {

        char * var = Tcl_GetString(objv[2]);
        const char * val = dbcapi.get_clientinfo(conn_state_ptr->conn, var);
        if ( val == NULL ) {
            Tcl_AppendResult(interp, "Session variable ", var, " does not exist.", NULL);
            return TCL_ERROR;
        }
        Tcl_SetObjResult(interp, Tcl_NewStringObj(val, -1));

    } else {

        Tcl_WrongNumArgs(interp, objc, objv, "set session_variable ?value?");
        return TCL_ERROR;
    }
    return TCL_OK;
}

/**
 * Calls DBCAPI to prepare SQL and then creates and returns the statement command to miltiplex the statement subcommands.
 */
static int
PrepareStmt (Conn_State * conn_state_ptr, Tcl_Interp * interp, const char * sql, Stmt_State * * stmt_state_ptr_ptr)
{
    Stmt_State * stmt_state_ptr = ckalloc(sizeof(Stmt_State));
    if ( stmt_state_ptr == NULL ) {
        Tcl_SetResult(interp, "cannot allocate memory for the statement internal state", TCL_STATIC);
        return TCL_ERROR;
    }
    memset(stmt_state_ptr, 0, sizeof(Stmt_State));
    stmt_state_ptr->conn_state_ptr = conn_state_ptr;

    stmt_state_ptr->stmt = dbcapi.prepare(conn_state_ptr->conn, sql);
    if ( stmt_state_ptr->stmt == NULL ) {
        SetErrorResult(interp, conn_state_ptr->conn, "Cannot prepare statement for execution", NULL);
        goto Error_Exit;
    }

    char name[24];
    int name_len = sprintf(name, "hdbstmt%" PRIxPTR, (uintptr_t) stmt_state_ptr->stmt);

    stmt_state_ptr->stmt_cmd = Tcl_CreateObjCommand(interp, name, (Tcl_ObjCmdProc *) Stmt_Cmd, (ClientData) stmt_state_ptr, (Tcl_CmdDeleteProc *) Stmt_DeleteState);
    if ( stmt_state_ptr->stmt_cmd == NULL ) {
        Tcl_SetResult(interp, "cannot create statement command handler", TCL_STATIC);
        goto Error_Exit;
    }
    int is_new;
    Tcl_HashEntry * entry = Tcl_CreateHashEntry(conn_state_ptr->open_statements, stmt_state_ptr->stmt_cmd, &is_new);
    Tcl_SetHashValue(entry, stmt_state_ptr->stmt_cmd);

    if ( stmt_state_ptr_ptr != NULL ) {
        *stmt_state_ptr_ptr = stmt_state_ptr;
    }

    Tcl_SetObjResult(interp, Tcl_NewStringObj(name, name_len));
    return TCL_OK;

Error_Exit:
    Stmt_DeleteState(stmt_state_ptr, interp);
    return TCL_ERROR;
}

/**
 * Prepares the SQL statement for execution. Returns the prepared statement, so it could be executed
 * later.
 *
 * # Example
 *
 * \code{.tcl}
 * set stmt [$conn prepare "INSERT INTO employees (employee_id, first_name, last_name) VALUES (?, ?, ?)"]
 * \endcode
 */
static int
Conn_Prepare (Conn_State * conn_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc != 3 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "prepare sql_string");
        return TCL_ERROR;
    }
    char * sql = Tcl_GetString(objv[2]);
    return PrepareStmt(conn_state_ptr, interp, sql, NULL);
}

/**
 * Prepares and executes the SQL statement. Returns the prepared statement, so it could be executed
 * again with different arguments.
 *
 * # Example
 *
 * \code{.tcl}
 * set stmt [$conn execute "INSERT INTO employees (employee_id, first_name, last_name) VALUES (?, ?, ?)" 2 "Hasso" "Plattner"]
 * \endcode
 */
static int
Conn_Execute (Conn_State * conn_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc < 3 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "execute sql_string ?arg...?");
        return TCL_ERROR;
    }
    char * sql = Tcl_GetString(objv[2]);
    Stmt_State * stmt_state_ptr;
    if ( PrepareStmt(conn_state_ptr, interp, sql, &stmt_state_ptr) != TCL_OK ) {
        return TCL_ERROR;
    }
    // Note that PrepareStmt has set the name of the statement command as result.
    // So, unless Stmt_Execute below resets it on error, that's what [$conn execute ...] will return

    if ( Stmt_Execute(stmt_state_ptr, interp, objc - 3, objv + 3) != TCL_OK ) {
        goto Error_Exit;
    }

    return TCL_OK;

Error_Exit:
    if ( Tcl_DeleteCommandFromToken(interp, stmt_state_ptr->stmt_cmd) != TCL_OK ) {
        Stmt_DeleteState(stmt_state_ptr, interp);
    }
    return TCL_ERROR;
}

/**
 * Commits the current transaction.
 *
 * # Example
 *
 * \code{.tcl}
 * $conn commit
 * \endcode
 */
static int
Conn_Commit (Conn_State * conn_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( !dbcapi.commit(conn_state_ptr->conn) ) {
        SetErrorResult(interp, conn_state_ptr->conn, "Cannot commit transaction", NULL);
        return TCL_ERROR;
    }
    return TCL_OK;
}

/**
 * Rolls back the current transaction.
 *
 * # Example
 *
 * \code{.tcl}
 * $conn rollback
 * \endcode
 */
static int
Conn_Rollback (Conn_State * conn_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( !dbcapi.rollback(conn_state_ptr->conn) ) {
        SetErrorResult(interp, conn_state_ptr->conn, "Cannot roll back transaction", NULL);
        return TCL_ERROR;
    }
    return TCL_OK;
}

/**
 * Connection subcommands multiplexor.
 */
static int
Conn_Cmd (Conn_State * conn_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc < 2 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "method ?option...?");
        return TCL_ERROR;
    }

    static const char * const methods[] = {
        "cget", "close", "commit", "configure", "execute", "prepare", "rollback", "set", NULL
    };
    enum {
        CGET, CLOSE, COMMIT, CONFIGURE, EXECUTE, PREPARE, ROLLBACK, SET
    } method;

    if ( Tcl_GetIndexFromObj(interp, objv[1], methods, "method", 0, (int *) &method) != TCL_OK ) {
        return TCL_ERROR;
    }
    switch ( method ) {
        case CONFIGURE:
            return Conn_Configure(conn_state_ptr, interp, objc, objv);
        case CGET:
            return Conn_Cget(conn_state_ptr, interp, objc, objv);
        case SET:
            return Conn_Set(conn_state_ptr, interp, objc, objv);
        case EXECUTE:
            return Conn_Execute(conn_state_ptr, interp, objc, objv);
        case PREPARE:
            return Conn_Prepare(conn_state_ptr, interp, objc, objv);
        case COMMIT:
            return Conn_Commit(conn_state_ptr, interp, objc, objv);
        case ROLLBACK:
            return Conn_Rollback(conn_state_ptr, interp, objc, objv);
        case CLOSE:
            return Conn_Close(conn_state_ptr, interp, objc, objv);
    }
    return TCL_OK;
}

/**
 * Establishes a database connection and returns the command that is used to manipulate the current connection.
 *
 * # Example
 *
 * \code{.tcl}
 * set conn [hdb connect -serverNode localhost:39041 -uid username -pwd password]
 * \endcode
 *
 * \see https://help.sap.com/viewer/0eec0d68141541d1b07893a39944924e/2.0.04/en-US/4fe9978ebac44f35b9369ef5a4a26f4c.html
 *      for a list of acceptable connection options/properties
 *
 * \note The `charset` option, if provided, will be overwritten and set to `UTF-8`
 */
static int
Hdb_Connect (Hdbtcl_State * hdbtcl_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc < 2 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "connect -option value ?-option value...?");
        return TCL_ERROR;
    }

    Conn_State * conn_state_ptr = ckalloc(sizeof(Conn_State));
    if ( conn_state_ptr == NULL ) {
        Tcl_SetResult(interp, "cannot allocate memory for the connection internal state", TCL_STATIC);
        return TCL_ERROR;
    }
    memset(conn_state_ptr, 0, sizeof(Conn_State));
    conn_state_ptr->hdbtcl_state_ptr = hdbtcl_state_ptr;

    conn_state_ptr->conn = dbcapi.new_connection();
    for ( int i = 0; i < objc; ) {
        char * opt_name  = Tcl_GetString(objv[i++]);
        char * opt_value = Tcl_GetString(objv[i++]);
        if ( opt_name == NULL || opt_value == NULL ) {
            Tcl_SetResult(interp, "malformed argument list", TCL_STATIC);
            goto Error_Exit;
        }
        if ( *opt_name == '-' ) {
            ++opt_name;
        }
        if ( !dbcapi.set_connect_property(conn_state_ptr->conn, opt_name, opt_value) ) {
            SetErrorResult(interp, conn_state_ptr->conn, "Cannot set connection property ", opt_name, NULL);
            goto Error_Exit;
        }
    }
    if ( !dbcapi.set_connect_property(conn_state_ptr->conn, "charset", "UTF-8") ) {
        SetErrorResult(interp, conn_state_ptr->conn, "Cannot configure character set for the connection", NULL);
        goto Error_Exit;
    }
    conn_state_ptr->connected = dbcapi.connect2(conn_state_ptr->conn);
    if ( !conn_state_ptr->connected ) {
        SetErrorResult(interp, conn_state_ptr->conn, "Cannot connect to the database", NULL);
        goto Error_Exit;
    }

    conn_state_ptr->open_statements = ckalloc(sizeof(Tcl_HashTable));
    Tcl_InitHashTable(conn_state_ptr->open_statements, TCL_ONE_WORD_KEYS);

    char name[24];
    int name_len = sprintf(name, "hdbconn%" PRIxPTR, (uintptr_t) conn_state_ptr);

    conn_state_ptr->conn_cmd = Tcl_CreateObjCommand(interp, name, (Tcl_ObjCmdProc *) Conn_Cmd, (ClientData) conn_state_ptr, (Tcl_CmdDeleteProc *) Conn_DeleteState);
    if ( conn_state_ptr->conn_cmd == NULL ) {
        Tcl_SetResult(interp, "cannot create connection command handler", TCL_STATIC);
        goto Error_Exit;
    }
    int is_new;
    Tcl_HashEntry * entry = Tcl_CreateHashEntry(hdbtcl_state_ptr->open_connections, conn_state_ptr->conn_cmd, &is_new);
    Tcl_SetHashValue(entry, conn_state_ptr->conn_cmd);

    Tcl_SetObjResult(interp, Tcl_NewStringObj(name, name_len));
    return TCL_OK;

Error_Exit:
    Conn_DeleteState(conn_state_ptr, interp);
    return TCL_ERROR;
}

/**
 * Implements the "hdb" command.
 *
 * "hdb" is a subcommand multiplexor. However, at the moment the only available subcommand is "connect".
 */
static int
Hdb_Cmd (Hdbtcl_State * hdbtcl_state_ptr, Tcl_Interp * interp, int objc, Tcl_Obj * const objv[])
{
    if ( objc < 2 ) {
        Tcl_WrongNumArgs(interp, objc, objv, "method ?option...?");
        return TCL_ERROR;
    }

    static const char * const methods[] = {
        "connect", NULL
    };
    enum {
        CONNECT
    } method;

    if ( Tcl_GetIndexFromObj(interp, objv[1], methods, "method", 0, (int *) &method) != TCL_OK ) {
        return TCL_ERROR;
    }
    switch ( method ) {
        case CONNECT:
            return Hdb_Connect(hdbtcl_state_ptr, interp, objc - 2, objv + 2);
    }
    return TCL_OK;
}

/**
 * Module state when loaded into the main interpreter.
 *
 * This one is needed to create a single DBCAPI cleanup point.
 */
typedef struct hdbtcl_main_state {
    Hdbtcl_State *  hdbtcl_state_ptr;
    Tcl_Interp *    main_interp;
} Hdbtcl_MainState;

/**
 * Possible types of exit handlers.
 */
static enum ExitHandlerState {
    NONE,       /// exit handler has not been set up yet
    INTERP,     /// module was loaded into the slave interpreter and the cleanup will happen when the latter terminates
    MAIN        /// module was loaded into the main interpreter and cleanup will run when main interpreter exits
} exit_handler_state = NONE;

/**
 * Module cleanup.
 *
 * Disposes of the module state and releases DBCAPI resources.
 */
static void
Hdbtcl_Finalize (Hdbtcl_MainState * main_state_ptr)
{
    if ( main_state_ptr != NULL ) {
        Hdbtcl_DeleteState(main_state_ptr->hdbtcl_state_ptr, main_state_ptr->main_interp);
        ckfree(main_state_ptr);
    }
    dbcapi.fini();
}

/**
 * Extension initialization entry.
 *
 * Initializes DBCAPI and the module state (mainly to track open connections).
 * Sets up state cleanup to run upon - main or slave - interpreter exit.
 */
int DLLEXPORT
Hdbtcl_Init (Tcl_Interp * interp)
{
#ifdef USE_TCL_STUBS
    if ( Tcl_InitStubs(interp, TCL_VERSION, 0) == NULL ) {
        return TCL_ERROR;
    }
#endif
    if ( dbcapi.init == NULL ) {
        if ( !init_dbcapi( getenv("HDBCAPILIB") ) ) {
            Tcl_SetResult(interp, "Cannot load DBCAPI library", TCL_STATIC);
            return TCL_ERROR;
        }
    }

    if ( !dbcapi.init("TCL", _DBCAPI_VERSION, NULL) ) {
        Tcl_SetResult(interp, "DBCAPI initialization failed", TCL_STATIC);
        return TCL_ERROR;
    }

    Hdbtcl_State * hdbtcl_state_ptr = Hdbtcl_InitState(interp);
    if ( hdbtcl_state_ptr == NULL ) {
        return TCL_ERROR;
    }

    Tcl_Obj * argc = Tcl_GetVar2Ex(interp, "argc", NULL, TCL_GLOBAL_ONLY);
    if ( argc == NULL ) {
        Tcl_CallWhenDeleted(interp, (Tcl_InterpDeleteProc *) Hdbtcl_DeleteState, (ClientData) hdbtcl_state_ptr);
        if ( exit_handler_state == NONE ) {
            Tcl_CreateExitHandler((Tcl_ExitProc *) Hdbtcl_Finalize, NULL);
            exit_handler_state = INTERP;
        }
    } else {
        Tcl_DecrRefCount(argc);
        if ( exit_handler_state == INTERP ) {
            Tcl_DeleteExitHandler((Tcl_ExitProc *) Hdbtcl_Finalize, NULL);
        }
        exit_handler_state = MAIN;
        Hdbtcl_MainState * main_state_ptr = ckalloc(sizeof(Hdbtcl_MainState));
        main_state_ptr->hdbtcl_state_ptr = hdbtcl_state_ptr;
        main_state_ptr->main_interp = interp;
        Tcl_CreateExitHandler((Tcl_ExitProc *) Hdbtcl_Finalize, (ClientData) main_state_ptr);
    }
    return Tcl_PkgProvide(interp, "hdbtcl", "1.0");
}
