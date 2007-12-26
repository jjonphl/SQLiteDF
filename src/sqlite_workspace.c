#include <stdio.h>
#include <string.h>

#define __SQLITE_WORKSPACE__
#include "sqlite_dataframe.h"

/* global variables */
sqlite3 *g_workspace = NULL;
char *g_sql_buf[NBUFS];
int g_sql_buf_sz[NBUFS];

/****************************************************************************
 * UTILITY FUNCTIONS
 ****************************************************************************/

/* test if a file is a sqlite database file */
sqlite3* _is_sqlitedb(const char *filename) {
    sqlite3 *db;
    int res;
    res = sqlite3_open(filename, &db);
    if (res != SQLITE_OK) { goto is_sqlitedb_FAIL; }

    sqlite3_stmt *stmt; char *sql = "select * from sqlite_master";
    res = sqlite3_prepare(db, sql, -1, &stmt, 0);
    if (stmt != NULL) sqlite3_finalize(stmt);
    /*char **result_set;
    char nrow, ncol;
    res = sqlite3_get_table(db, "select * from sqlite_master limit 0", 
            &result_set, &nrow, &ncol, NULL);
    sqlite3_free_table(result_set);*/
    if (res != SQLITE_OK) goto is_sqlitedb_FAIL;

    return db;

is_sqlitedb_FAIL:
    sqlite3_close(db);
    return NULL;
}

/* test if a file is a SQLiteDF workspace */
sqlite3* _is_workspace(char *filename) {
    sqlite3* db = _is_sqlitedb(filename); 

    if (db != NULL) {
        sqlite3_stmt *stmt;
        char *sql = "select * from workspace";
        int res = sqlite3_prepare(db, sql, -1, &stmt, 0), ncols;
        if ((res != SQLITE_OK) || /* no workspace table */
              ((ncols = sqlite3_column_count(stmt)) != WORKSPACE_COLUMNS) ||
              /* below also checks the ordering of the columns */
              (strcmp(sqlite3_column_name(stmt, 0), "rel_filename") != 0) ||
              (strcmp(sqlite3_column_decltype(stmt, 0), "text") != 0) ||
              (strcmp(sqlite3_column_name(stmt, 1), "full_filename") != 0) ||
              (strcmp(sqlite3_column_decltype(stmt, 1), "text") != 0) ||
              (strcmp(sqlite3_column_name(stmt, 2), "internal_name") != 0) ||
              (strcmp(sqlite3_column_decltype(stmt, 2), "text") != 0) ||
              (strcmp(sqlite3_column_name(stmt, 3), "loaded") != 0) ||
              (strcmp(sqlite3_column_decltype(stmt, 3), "bit") != 0) ||
              (strcmp(sqlite3_column_name(stmt, 4), "uses") != 0) ||
              (strcmp(sqlite3_column_decltype(stmt, 4), "int") != 0) ||
              (strcmp(sqlite3_column_name(stmt, 5), "used") != 0) ||
              (strcmp(sqlite3_column_decltype(stmt, 5), "bit") != 0)) {
            sqlite3_finalize(stmt); sqlite3_close(db); db = NULL;
        } else {
            sqlite3_finalize(stmt);
        }
    }

    return db;
}

/* test if a file is a sqlite.data.frame. returns the internal name of the
 * sdf if file is an sdf or NULL otherwise */
const char * _is_sdf2(const char *filename) {
    sqlite3* db = _is_sqlitedb(filename); 
    const char *ret = (db == NULL) ? NULL : filename;

    if (ret) {
        sqlite3_stmt *stmt;
        char *sql = "select * from sdf_attributes where attr='name'";
        int res, ncols;
        res = sqlite3_prepare(db, sql, -1, &stmt, NULL);
        ret = (((res == SQLITE_OK) && /* no attribute table */
               ((ncols = sqlite3_column_count(stmt)) == 2) &&
               (strcmp(sqlite3_column_name(stmt, 0), "attr") == 0) &&
               (strcmp(sqlite3_column_decltype(stmt, 0), "text") == 0) &&
               (strcmp(sqlite3_column_name(stmt, 1), "value") == 0) &&
               (strcmp(sqlite3_column_decltype(stmt, 1), "text") == 0))) ? ret : NULL ;
        
        if (ret == NULL) goto _is_sdf_cleanup;

        /* get internal name. we are assuming here that the 1st row of the attributes
         * table always contains the column name */
        res = sqlite3_step(stmt);
        ret = (res == SQLITE_ROW) ? ret : NULL;
        if (ret == NULL) goto _is_sdf_cleanup;

        /* copy to buf2, because when we finalize stmt, we won't be sure
         * if sqlite3_column_text()'s ret value will still be there */
        strcpy(g_sql_buf[2], (char *)sqlite3_column_text(stmt, 1));
        ret = g_sql_buf[2];
        sqlite3_finalize(stmt);
        
        sql = "select * from sdf_data";
        res = sqlite3_prepare(db, sql, -1, &stmt, NULL);
        ret = (res == SQLITE_OK) ? ret : NULL;  /* if not, missing data table */

_is_sdf_cleanup:
        sqlite3_finalize(stmt);
        sqlite3_close(db);
    }

    return ret;
}

/* remove an sdf from the workspace */
void _delete_sdf2(const char *iname) {
    sprintf(g_sql_buf[2], "delete from workspace where internal_name='%s';", iname);
    _sqlite_exec(g_sql_buf[2]);
}

/* add a sdf to the workspace */
int _add_sdf1(const char *filename, const char *internal_name) {
    sprintf(g_sql_buf[1], "insert into workspace(rel_filename, full_filename, "
            "internal_name, loaded, uses, used) values('%s', '%s', '%s', 0, 0, 0)",
            filename, _get_full_pathname2(filename), internal_name);

    return _sqlite_exec(g_sql_buf[1]);
}


static char* _get_sdf_detail2(const char *iname, int what) {
    sqlite3_stmt *stmt;
    char * ret; int res;

    sprintf(g_sql_buf[2], "select full_filename from workspace where "
            "internal_name='%s'", iname);
    sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, NULL);
    res = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (res == SQLITE_DONE) {
        ret = NULL;
    } else {
        ret = g_sql_buf[2];
        switch (what) {
            case SDF_DETAIL_EXISTS: 
                break; /* doesn't matter */
            case SDF_DETAIL_FULLFILENAME:
                strcpy(g_sql_buf[2], (char *)sqlite3_column_text(stmt, 0));
                break;
        }
    }

    return ret;
}

/* returns TRUE if sdf exists in the workspace */
int _sdf_exists2(const char *iname) {
    return _get_sdf_detail2(iname, SDF_DETAIL_EXISTS) != NULL;
}

/****************************************************************************
 * WORKSPACE FUNCTIONS
 ****************************************************************************/

SEXP sdf_init_workspace() {
    int file_idx = 0, i;
    char *basename = "workspace", *filename;
    SEXP ret;

    /* initialize sql_buf */
    for (i = 0; i < NBUFS; i++) {
        if (g_sql_buf[i] == NULL) {
            g_sql_buf_sz[i] = 1024;
            g_sql_buf[i] = Calloc(g_sql_buf_sz[i], char);
        }
    }

    /* create symbols used for object attributes */
    SDF_RowNamesSymbol = install("sdf.row.names");
    SDF_VectorTypeSymbol = install("sdf.vector.type");
    SDF_DimSymbol = install("sdf.dim");
    SDF_DimNamesSymbol = install("sdf.dimnames");

    /*
     * check for workspace.db, workspace1.db, ..., workspace9999.db if they
     * are valid workspace file. if one is found, use that as the workspace.
     */
    filename = R_alloc(28, sizeof(char)); /* .SQLiteDF/workspace10000.db\0 */
    sprintf(filename, ".SQLiteDF/%s.db", basename);
    while(_file_exists(filename) && file_idx < 10000) {
        if ((g_workspace = _is_workspace(filename)) != NULL) break;
        warning("%s is not a SQLiteDF workspace\n", filename);
        sprintf(filename, ".SQLiteDF/%s%d.db", basename, ++file_idx);
    }

    if ((g_workspace == NULL) && (file_idx < 10000)) {
        /* no workspace found but there are still "available" file name */
        /* if (file_idx) warn("workspace will be stored at #{filename}") */
        sqlite3_open(filename, &g_workspace);
        _sqlite_exec("create table workspace(rel_filename text, full_filename text,"
               "internal_name text unique, loaded bit, uses int, used bit)");
        ret = ScalarLogical(TRUE);
    } else if (g_workspace != NULL) {
        /* a valid workspace has been found, load each of the tables */
        int res, nrows, ncols; 
        char **result_set, *fname, *iname;
        
        /* since only 30 can be loaded, sort sdf's by # of uses the last time */
        res = sqlite3_get_table(g_workspace, "select rel_filename, internal_name"
                " from workspace order by uses desc", 
                &result_set, &nrows, &ncols, NULL);

        /* reset fields uses and loaded to 0 and false */
        res = _sqlite_exec("update workspace set uses=0, loaded=0, used=0");
        _sqlite_error(res);
        
        if (res == SQLITE_OK && nrows >= 1 && ncols == 2) {
            int maxrows;  /* actual # of sdfs to be attached */
            maxrows = (nrows > 30) ? 30 : nrows;
            for (i = 1; i <= maxrows && i < nrows; i++) {
                /* we will use rel_filename in opening the file, so that
                 * if the user is "sensible", files will be dir agnostic */
                fname = result_set[i*ncols]; iname = result_set[i*ncols+1];

                if (!USE_SDF1(iname, TRUE, FALSE)) maxrows++;

                /* update full_filename */
                sprintf(g_sql_buf[0], "update workspace set full_filename='%s' "
                        "where iname='%s'", _get_full_pathname2(fname), iname);
                _sqlite_exec(g_sql_buf[0]);
            }
        }
        sqlite3_free_table(result_set);

        /* notify if a previous workspace is reloaded */
        Rprintf("[Previous SQLiteDF workspace restored (%s)]\n", filename);

        ret = ScalarLogical(TRUE);
    } else { /* can't find nor create workspace */
        ret = ScalarLogical(FALSE);
    }

    /* register sqlite math functions */
    __register_vector_math();
    return ret;
}
        
int USE_SDF1(const char *iname, int exists, int protect) {
    sqlite3_stmt *stmt;
    int loaded, res;
    char *iname_final = (char *)iname;

    /* determine if iname is loaded */
    sprintf(g_sql_buf[2], "select loaded, rel_filename from workspace where internal_name='%s'", iname);
    sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, NULL);
    res = sqlite3_step(stmt);
    if (exists && res != SQLITE_ROW) { 
        sqlite3_finalize(stmt); 
        error("No SDF with name '%s' found in the workspace.\n", iname);
    }
    loaded = (res == SQLITE_ROW) ? sqlite3_column_int(stmt, 0) : 0;
    strcpy(g_sql_buf[1], (char*)sqlite3_column_text(stmt, 1)); /* ref filename */
    sqlite3_finalize(stmt);

    if (!loaded) {
        char *fname = g_sql_buf[1];

        /* test first if we will be loading valid sdf */
        if (exists && !_file_exists(fname)) {
            _delete_sdf2(iname);
            warning("SDF %s does not exist.\n", iname);
            return 0;
        }

        if (exists && _is_sdf2(fname) == NULL) {
            _delete_sdf2(iname);
            warning("%s is not a valid SDF.\n", fname);
            return 0;
        }

        /* g_sql_buf[2] contains the internal name as stored in the sdf
         * attribute. sync workspace record to that if needed */
        if (exists && strcmp(iname, g_sql_buf[2]) != 0) {
            if (!_is_r_sym(g_sql_buf[2])) {
                warning("name \"%s\" stored in SDF is not valid. Ignoring that name...", g_sql_buf[2]);
                goto __out_of_syncname;
            }
            iname_final = R_alloc(strlen(g_sql_buf[2]) + 1, sizeof(g_sql_buf[2]));
            strcpy(iname_final, g_sql_buf[2]);

            sprintf(g_sql_buf[2], "update workspace set internal_name='%s' where "
                        "internal_name='%s'", iname_final, iname);
            _sqlite_error(_sqlite_exec(g_sql_buf[2]));

        }
__out_of_syncname:

        /* unload sdf's if we run to the MAX_ATTACHED limit */
        _prepare_attach2();

        /* set loaded to true */
        sprintf(g_sql_buf[2], "update workspace set loaded=1 where internal_name='%s'", iname_final);
        res = _sqlite_exec(g_sql_buf[2]);
        _sqlite_error(res);

        /* load, using relative path */
        sprintf(g_sql_buf[2], "attach '%s' as [%s]", fname, iname_final);
        res = _sqlite_exec(g_sql_buf[2]);
        if (_sqlite_error(res)) return 0;
    }

    /* upgrade its uses count, and protect if necessary */
    if (protect) protect = 1; 
    sprintf(g_sql_buf[2], "update workspace set uses=uses+1, used=%d" 
            " where internal_name='%s'", protect, iname_final);
    _sqlite_exec(g_sql_buf[2]);
    return 1;
}

int UNUSE_SDF2(const char *iname) {
    sprintf(g_sql_buf[2], "update workspace set used=0 where internal_name='%s'", iname);
    _sqlite_exec(g_sql_buf[2]);
    return 1;
}

        
    
SEXP sdf_finalize_workspace() {
    SEXP ret;
    int i;
    ret = ScalarLogical(sqlite3_close(g_workspace) == SQLITE_OK);
    for (i = 0; i < NBUFS; i++) Free(g_sql_buf[i]);
    return ret;
} 


SEXP sdf_list_sdfs(SEXP pattern) {
    SEXP ret;
    char **result;
    int nrow, ncol, res, i;

    if (TYPEOF(pattern) != STRSXP) {
        res = sqlite3_get_table(g_workspace, "select internal_name from workspace "
                "order by loaded desc, uses desc", &result, &nrow, &ncol, NULL);
    } else {
        /* since internal_names must be a valid r symbol, 
           did not check for "'" */
        sprintf(g_sql_buf[0], "select internal_name from workspace where "
                "internal_name like '%s%%'", CHAR(STRING_ELT(pattern, 0)));
        res = sqlite3_get_table(g_workspace, g_sql_buf[0], &result, &nrow,
                &ncol, NULL);
    }

    if (_sqlite_error(res)) return R_NilValue;
    PROTECT(ret = NEW_CHARACTER(nrow));
    
    for (i = 0; i < nrow; i++) SET_STRING_ELT(ret, i, mkChar(result[i+1]));

    sqlite3_free_table(result);
    UNPROTECT(1);
    return ret;
}

SEXP sdf_get_sdf(SEXP name) {    
    const char *iname;
    SEXP ret;

    if (TYPEOF(name) != STRSXP) {
        error("Argument must be a string containing the SDF name.");
    }
    iname = CHAR(STRING_ELT(name, 0));

    if (!USE_SDF1(iname, TRUE, FALSE)) return R_NilValue;

    ret = _create_sdf_sexp(iname);
    return ret;
}

SEXP sdf_attach_sdf(SEXP filename, SEXP internal_name) {
    /* when studying this, please be mindful of the global buffers used.
     * you have been warned */
    const char *fname=NULL, *iname, *iname_orig=NULL;
    int fnamelen=0, res;
    sqlite3_stmt *stmt;

    if (IS_CHARACTER(filename)) {
        fname = CHAR_ELT(filename, 0);
        fnamelen = strlen(fname);
    } else {
        error("filename argument must be a string.");
    }

    if (strcmp(fname+(fnamelen-3),".db") != 0) {
        error("will not attach because extension is not .db");
    }

    /* check if it is a valid sdf file */
    if (_is_sdf2(fname) == NULL) {
        error("%s is not a valid SDF.", fname);
    } else {
        /* _is_sdf2 puts the orig iname in buf2. transfer data to buf0 since
         * functions called below will use buf2 */
        strcpy(g_sql_buf[0], g_sql_buf[2]);
        iname_orig = g_sql_buf[0];
    }

    /* check if file to be attached exists in the workspace already */
    _get_full_pathname2(fname);
    res = sqlite3_prepare(g_workspace, "select internal_name from workspace where full_filename=?",
            -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, g_sql_buf[2], -1, SQLITE_STATIC);
    res = sqlite3_step(stmt);
    if (res == SQLITE_ROW) {
        warning("this sdf is already attached as '%s'\n",
                sqlite3_column_text(stmt, 0));
        sqlite3_finalize(stmt);
        return R_NilValue;
    } else sqlite3_finalize(stmt);


    /* internal_name checking and processing. */
    if (IS_CHARACTER(internal_name)) {
        /* if name is specified, rename the sdf. original internal name is the
         * one stored at sdf_attribute */
        iname = CHAR_ELT(internal_name, 0);
        if (!_is_r_sym(iname)) {
            error("%s is not a valid R symbol.", iname);
        }
    } else {
        /* if no name is specified, use original internal name */
        iname = (char *)iname_orig;  /* g_sql_buf[0]! */
    }

    /* check if internal name is already used in the workspace */
    res = sqlite3_prepare(g_workspace, "select full_filename from workspace "
           " where internal_name=?", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, iname, -1, SQLITE_STATIC);
    res = sqlite3_step(stmt);
    if (res == SQLITE_ROW) {
        strcpy(g_sql_buf[0], (char *)sqlite3_column_text(stmt,1));
        sqlite3_finalize(stmt);
        error("the sdf internal name '%s' is already used by file %s.",
                iname, g_sql_buf[0]);
    } 
    sqlite3_finalize(stmt);
    
    /* add it to workspace */
    res = _add_sdf1(fname, iname);
    if (_sqlite_error(res)) return R_NilValue;

    /* attach using USE_SDF1, detach other SDF if necessary */
    USE_SDF1(iname, TRUE, FALSE);

    /* if internal name found in newly-attached-SDF is the same as the
     * name wanted by the user, do nothing. otherwise, update sdf_attribute
     * on attached-SDF. this is like attachSdf then renameSdf */
    if (iname != iname_orig && strcmp(iname, iname_orig) != 0) {
        sprintf(g_sql_buf[1], "update [%s].sdf_attributes set value=? where attr='name'",
                iname);
        res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, NULL);
        if (_sqlite_error(res)) { 
            sqlite3_finalize(stmt); 
            sprintf(g_sql_buf[1], "detach [%s]", iname);
            _sqlite_exec(g_sql_buf[1]);
            return R_NilValue;
        }
        res = sqlite3_bind_text(stmt, 1, iname, -1, SQLITE_STATIC);
        res = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    } 

    return _create_sdf_sexp(iname);
}

/* not necessary anymore, since stuffs will eventually be detached
 * if we keep on adding new sdfs */
SEXP sdf_detach_sdf(SEXP internal_name) {
    const char *iname;
    int res;

    if (!IS_CHARACTER(internal_name)) {
        error("iname argument is not a string.");
    }

    iname = CHAR_ELT(internal_name, 0);
    sprintf(g_sql_buf[0], "detach [%s]", iname);

    res = _sqlite_exec(g_sql_buf[0]);
    res = !_sqlite_error(res);

    if (res) _delete_sdf2(iname);
    
    return ScalarLogical(res);
}

SEXP sdf_rename_sdf(SEXP sdf, SEXP name) {
    const char *iname, *newname;
    char *path; 
    int res, ret_tmp;

    iname = SDF_INAME(sdf);
    newname = CHAR_ELT(name, 0);
    
    if (!USE_SDF1(iname, TRUE, FALSE)) return R_NilValue;

    /* check if valid r name */
    if (!_is_r_sym(newname)) {
        error("%s is not a valid R symbol.", iname);
    }

    /* check if sdf already exists */
    if (_sdf_exists2(newname)) { /* name is already taken */
        error("Error: the name \"%s\" is already taken.", newname);
    }

    /* get path of the sdf file, because we're going to detach it */
    path = _get_sdf_detail2(iname, SDF_DETAIL_FULLFILENAME);
    if (path == NULL) {
        error("no sdf named \"%s\" exists.", iname);
    }

    /* change name in sdf_attribute */
    sprintf(g_sql_buf[0], "update [%s].sdf_attributes set value='%s' "
            "where attr='name'", iname, newname);
    res = _sqlite_exec(g_sql_buf[0]);
    /* if (_sqlite_error(res)) return R_NilValue; */

    /* detach and remove sdf from workspace */
    sprintf(g_sql_buf[0], "detach '%s'", iname);
    res = _sqlite_exec(g_sql_buf[0]);
    ret_tmp = !_sqlite_error(res);

    /* remove from ws, attach and add again to ws using new name */
    if (ret_tmp) {
        _delete_sdf2(iname);
        sprintf(g_sql_buf[0], "attach '%s' as '%s'", path, newname);
        res = _sqlite_exec(g_sql_buf[0]);
        ret_tmp = !_sqlite_error(res);
        
        /* TODO: make path relative! */
        _add_sdf1(iname, path);
    }

    return ScalarLogical(ret_tmp);
}

