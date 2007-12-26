#include <stdio.h>
#include <string.h>
#include "sqlite_dataframe.h"


SEXP sdf_import_sqlite_table(SEXP _dbfilename, SEXP _tblname, SEXP _sdfiname) {
    sqlite3 *db;
    sqlite3_stmt *stmt;
    int res, nrows, ncols, buflen0, buflen1, buflen3, i;
    int tblname_len, colname_len, iname_len;
    const char *dbfilename, *tblname;
    char *iname, *type;
    const char *colname;
    SEXP ret = NULL;

    dbfilename = CHAR_ELT(_dbfilename, 0);
    tblname = CHAR_ELT(_tblname, 0);
    /* determine if we were passed a real sqlite db */
    db = _is_sqlitedb(dbfilename);

    if (!db) {
        Rprintf("Error: %s is not a SQLite database.\n", dbfilename);
        return R_NilValue;
    }

    /* check if the table exists in the db */
    sprintf(g_sql_buf[0], "select count(*) from [%s]", tblname);
    res = sqlite3_prepare(db, g_sql_buf[0], -1, &stmt, 0);
    if (_sqlite_error(res)) { 
        sqlite3_finalize(stmt); sqlite3_close(db);
        return R_NilValue; 
    }
    sqlite3_step(stmt);
    nrows = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt); sqlite3_close(db);
    if (nrows == 0) {
        Rprintf("Error: Table contains no rows. Aborting import operation.\n");
        return R_NilValue;
    }

    /* we have to attach it so we can transfer easily */

    /* unload sdf's if we run to the MAX_ATTACHED limit */
    _prepare_attach2();

    /* attach the table to be imported */
    sprintf(g_sql_buf[0], "attach '%s' as [@@import@@]", dbfilename);
    res = _sqlite_exec(g_sql_buf[0]);
    if (_sqlite_error(res)) return R_NilValue;

    
    /* create the sdf where the data will be imported to */
    iname = _create_sdf_skeleton1(_sdfiname, NULL, FALSE);
    if (iname == NULL) goto sdf_import_sqlite_table__out;

    iname_len = strlen(iname);  /* needed for buf resizing below */
    tblname_len = strlen(tblname);

    /* get column types and names */
    sprintf(g_sql_buf[0], "select * from [@@import@@].[%s]", tblname);
    res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0);
    _sqlite_error(res);
    sqlite3_step(stmt);
    ncols = sqlite3_column_count(stmt);

    /* create the sql scripts to execute: the create table and the insert-select */
    buflen0 = sprintf(g_sql_buf[0], "create table [%s].sdf_data([row name] text", iname);
    buflen1 = sprintf(g_sql_buf[1], "select cumsum(1)");
    buflen3 = sprintf(g_sql_buf[3], "from [@@import@@].[%s] ", tblname);
    for (i = 0; i < ncols; i++) {
        type = _str_tolower(g_sql_buf[2], sqlite3_column_decltype(stmt, i));
        colname = sqlite3_column_name(stmt, i);
        colname_len = strlen(colname);
        if (strstr(type, "int")) {
            _expand_buf(0, buflen0 + strlen(colname) + 10);
            buflen0 += sprintf(g_sql_buf[0]+buflen0, ",[%s] int", colname);

            _expand_buf(1, buflen1 + colname_len + 10);
            buflen1 += sprintf(g_sql_buf[1]+buflen1, ",[%s]", colname);
        } else if (strstr(type, "char") || strcmp(type, "text") == 0) {
            _expand_buf(0, buflen0 + strlen(colname) + 10);
            buflen0 += sprintf(g_sql_buf[0]+buflen0, ",[%s] int", colname);

            _expand_buf(1, buflen1 + colname_len + iname_len + 20);
            buflen1 += sprintf(g_sql_buf[1]+buflen1, ",[%s].[factor %s].level",
                   iname, colname);

            /* create factor table */
            _create_factor_table2(iname, "factor", colname);

            /* insert factor levels and labels */
            sprintf(g_sql_buf[2], "insert into [%s].[factor %s] (label)"
                   " select distinct [%s] from [@@import@@].[%s]", 
                   iname, colname, colname, tblname);
            res = _sqlite_exec(g_sql_buf[2]);
            _sqlite_error(res);

            _init_sqlite_function_accumulator();
            sprintf(g_sql_buf[2], "update [%s].[factor %s] set level=cumsum(1)",
                   iname, colname);
            res = _sqlite_exec(g_sql_buf[2]);
            _sqlite_error(res);

            /* add join clause to from clause */
            _expand_buf(3, buflen3 + colname_len * 3 + iname_len * 2 + strlen(tblname) + 32);
            buflen3 += sprintf(g_sql_buf[3] + buflen3, " join [%s].[factor %s] "
                    "on [@@import@@].[%s].[%s] = [%s].[factor %s].label",
                    iname, colname, tblname, colname, iname, colname);
        } else if (strcmp(type, "blob") == 0 || !*type) {
            /* not supported */
            warning("skipping column %s because it is a blob.\n", colname);
        } else { /* numeric / float / real / ... */
            _expand_buf(0, buflen0 + strlen(colname) + 10);
            buflen0 += sprintf(g_sql_buf[0]+buflen0, ",[%s] double", colname);

            _expand_buf(1, buflen1 + colname_len + 10);
            buflen1 += sprintf(g_sql_buf[1]+buflen1, ",[%s]", colname);
        }
    }

    sqlite3_finalize(stmt);

    /* execute create script */
    _expand_buf(0, buflen0 + 10);
    sprintf(g_sql_buf[0] + buflen0, ")");
    res = _sqlite_exec(g_sql_buf[0]);
    if (_sqlite_error(res)) {
        sdf_detach_sdf(mkString(iname));
        goto sdf_import_sqlite_table__out;
    }

    /* execute insert-select script */
    _init_sqlite_function_accumulator();
    _expand_buf(0, buflen1 + buflen3 + iname_len + 32 );
    sprintf(g_sql_buf[0], "insert into [%s].sdf_data %s %s", iname, g_sql_buf[1], g_sql_buf[3]);
    res = _sqlite_exec(g_sql_buf[0]);
    if (_sqlite_error(res)) {
        sdf_detach_sdf(mkString(iname));
        goto sdf_import_sqlite_table__out;
    }

    /* create sexp sdf */
    ret = _create_sdf_sexp(iname);

sdf_import_sqlite_table__out:
    _sqlite_exec("detach [@@import@@]");
    return ret;
}

SEXP sdf_tempdir() {
    SEXP tempdir, ans;

    PROTECT(tempdir = allocList(1));
    SET_TYPEOF(tempdir, LANGSXP);
    SETCAR(tempdir,install("tempdir"));
    /* SET_VECTOR_ELT(tempdir, 0, install("tempdir")); */
    ans = eval(tempdir, R_GlobalEnv);
    
    UNPROTECT(1);
    return ans;
}
