#define __SQLITE_MATRIX__
#include "sqlite_dataframe.h"

/****************************************************************************
 * UTILITY FUNCTIONS
 ****************************************************************************/
static SEXP _create_smat_sexp(const char *mat_iname, const char *type, 
        int nrows, int ncols, SEXP colnames) {
    SEXP ret, tmp;
    int i;

    /* return smat sexp */
    PROTECT(ret = NEW_LIST(3)); i = 1; /* 1 for names sexp above */
    SET_VECTOR_ELT(ret, 0, mkString(mat_iname));
    SET_VECTOR_ELT(ret, 1, mkString("sdf_data"));
    SET_VECTOR_ELT(ret, 2, mkString("V1"));

    /* set smat data name */
    PROTECT(tmp = NEW_CHARACTER(3)); i++;
    SET_STRING_ELT(tmp, 0, mkChar("iname"));
    SET_STRING_ELT(tmp, 1, mkChar("tblname"));
    SET_STRING_ELT(tmp, 2, mkChar("varname"));
    SET_NAMES(ret, tmp);

    /* set class */
    PROTECT(tmp = mkString("sqlite.matrix")); i++;
    SET_CLASS(ret, tmp);

    /* set smat dim */
    PROTECT(tmp = NEW_INTEGER(2)); i++;
    INTEGER(tmp)[0] = nrows;
    INTEGER(tmp)[1] = ncols;  /* mind [row names], w/c is not included in the count */
    setAttrib(ret, SDF_DimSymbol, tmp);

    /* set smat dimname */
    PROTECT(tmp = NEW_LIST(2)); i++;
    SET_VECTOR_ELT(tmp, 0, _create_svector_sexp(mat_iname, 
                "sdf_matrix_rownames", "name", "character"));
    SET_VECTOR_ELT(tmp, 1, colnames);
    setAttrib(ret, SDF_DimNamesSymbol, tmp);

    /* set sdf.vector.type */
    setAttrib(ret, SDF_VectorTypeSymbol, mkString(type));

    UNPROTECT(i);

    return ret;
}

/****************************************************************************
 * SMAT FUNCTIONS
 ****************************************************************************/
SEXP sdf_as_matrix(SEXP sdf, SEXP name) {
    char *mat_iname, *type;
    const char *iname, *dectype, *colname, *vectype = NULL;
    sqlite3_stmt *stmt, *stmt2;
    int ncols, nrows, i;
    SEXP ret, names;

    iname = SDF_INAME(sdf);

    if (!USE_SDF1(iname, TRUE, TRUE)) return R_NilValue;

    /* check column types, and determine matrix mode */
    sprintf(g_sql_buf[0], "select * from [%s].sdf_data", iname);
    sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0);
    sqlite3_step(stmt);

    ncols = sqlite3_column_count(stmt);
    type = NULL;
    for (i = 1; i < ncols; i++) {
        dectype = sqlite3_column_decltype(stmt, i);
        colname = sqlite3_column_name(stmt, i);
        if (strcmp(dectype, "double") == 0) {
            type = "double"; vectype = "numeric";
        } else if (strcmp(dectype, "int") == 0) {
            if (_is_factor2(iname, "factor", colname) || _is_factor2(iname, "ordered", colname)) {
                type = "text"; vectype = "character"; break;
            } 
            type = "double";
        } else if (strcmp(dectype, "bit") == 0) {
            if (type == NULL) { type = "bit"; vectype = "logical";  }
        } else if (strcmp(dectype, "text") == 0) {
            type = "text"; vectype = "character"; break;
        }
    }


    /* create sdf with 1 column */
    mat_iname = _create_svector1(name, type, NULL, TRUE);

    /* create and set sdf_matrix_rownames & sdf_matrix_colnames */
    sprintf(g_sql_buf[0], "create table [%s].sdf_matrix_rownames(name text)", mat_iname);
    _sqlite_error(_sqlite_exec(g_sql_buf[0]));
    sprintf(g_sql_buf[0], "insert into [%s].sdf_matrix_rownames "
            "select [row name] from [%s].sdf_data", mat_iname, iname);
    _sqlite_error(_sqlite_exec(g_sql_buf[0]));
    sprintf(g_sql_buf[0], "create table [%s].sdf_matrix_colnames(name text)", mat_iname);
    _sqlite_error(_sqlite_exec(g_sql_buf[0]));
    sprintf(g_sql_buf[0], "insert into [%s].sdf_matrix_colnames values (?)", mat_iname);
    sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt2, 0);

    /* store names in a SEXP */
    names = NEW_CHARACTER(ncols - 1);
    
    /* insert cast-ed values, and column names */
    nrows = _get_row_count2(iname, TRUE);
    _sqlite_begin;
    for (i = 1; i < ncols; i++) {
        colname = sqlite3_column_name(stmt, i);

        sqlite3_reset(stmt2);
        sqlite3_bind_text(stmt2, 1, colname, -1, SQLITE_STATIC);
        sqlite3_step(stmt2);

        SET_STRING_ELT(names, i-1, mkChar(colname));

        if ((_is_factor2(iname, "ordered", colname) && ((dectype = "ordered"))) || 
            ( _is_factor2(iname, "factor", colname) && ((dectype = "factor")))) {
            sprintf(g_sql_buf[0], "insert into [%s].sdf_data([row name], V1) "
                    "select [row name]|| %d, [%s].[%s %s].label from [%s].sdf_data "
                    "join [%s].[%s %s] on [%s].sdf_data.[%s]=[%s].[%s %s].level",
                    mat_iname, i, iname, dectype, colname, iname, iname, dectype, 
                    colname, iname, colname, iname, dectype, colname);
        } else {
            sprintf(g_sql_buf[0], "insert into [%s].sdf_data([row name], V1) "
                    "select [row name] || %d, cast([%s] as %s) from [%s].sdf_data", mat_iname, i,
                    colname, type, iname);
        }
        _sqlite_error(_sqlite_exec(g_sql_buf[0]));
    }
    sqlite3_finalize(stmt2);
    sqlite3_finalize(stmt);
    _sqlite_commit;

    /* return smat sexp */
    ret = _create_smat_sexp(mat_iname, vectype, nrows, ncols - 1, names);

    UNUSE_SDF2(iname);
    UNUSE_SDF2(mat_iname);
    return ret;
}

/* "decorate" passed svec w/ rownames & colnames to make it a sqlite.matrix */
SEXP sdf_create_smat(SEXP svec, SEXP dimnames) {
    SEXP tmp, ret = svec;
    sqlite3_stmt *stmt;
    const char *mat_iname;
    int i, nrows, ncols;

    mat_iname = SDF_INAME(svec);
    if (!USE_SDF1(mat_iname, TRUE, TRUE)) return R_NilValue;

    /* create and set sdf_matrix_rownames */
    sprintf(g_sql_buf[0], "create table [%s].sdf_matrix_rownames(name text)", mat_iname);
    _sqlite_error(_sqlite_exec(g_sql_buf[0]));

    sprintf(g_sql_buf[0], "insert into [%s].sdf_matrix_rownames(name) values (?)", mat_iname);
    sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, NULL);
    tmp = VECTOR_ELT(dimnames, 0);

    if (inherits(tmp, "sqlite.vector")) {
        const char *iname = SDF_INAME(tmp), *tblname = SVEC_TBLNAME(tmp),
                   *varname = SVEC_VARNAME(tmp);
        USE_SDF1(iname, TRUE, TRUE);
        nrows = _get_row_count2(iname, TRUE);
        sprintf(g_sql_buf[0], "insert into [%s].sdf_matrix_rownames(name) "
                "select [%s] from [%s].[%s]", mat_iname, varname, iname, tblname);
        _sqlite_error(_sqlite_exec(g_sql_buf[0]));
        UNUSE_SDF2(iname);
    } else {
        nrows = LENGTH(tmp);
        _sqlite_begin;
        for (i = 0; i < nrows; i++) {
            sqlite3_reset(stmt);
            sqlite3_bind_text(stmt, 1, CHAR_ELT(tmp, i), -1, SQLITE_STATIC);
            sqlite3_step(stmt);
        }
        _sqlite_commit;
    }

    /* create and set sdf_matrix_colnames */
    sprintf(g_sql_buf[0], "create table [%s].sdf_matrix_colnames(name text)", mat_iname);
    _sqlite_error(_sqlite_exec(g_sql_buf[0]));

    sprintf(g_sql_buf[0], "insert into [%s].sdf_matrix_rownames(name) values (?)", mat_iname);
    sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, NULL);
    tmp = VECTOR_ELT(dimnames, 1);
    ncols = LENGTH(tmp);
    _sqlite_begin;
    for (i = 0; i < ncols; i++) {
        sqlite3_reset(stmt);
        sqlite3_bind_text(stmt, 1, CHAR_ELT(tmp, i), -1, SQLITE_STATIC);
        sqlite3_step(stmt);
    }
    _sqlite_commit;

    /* modify the passed svec sexp to smat */
    /* set class */
    PROTECT(tmp = mkString("sqlite.matrix")); i = 1;
    SET_CLASS(ret, tmp);

    /* set smat dim */
    PROTECT(tmp = NEW_INTEGER(2)); i++;
    INTEGER(tmp)[0] = nrows;
    INTEGER(tmp)[1] = ncols; 
    setAttrib(ret, SDF_DimSymbol, tmp);

    /* set smat dimname */
    PROTECT(tmp = NEW_LIST(2)); i++;
    SET_VECTOR_ELT(tmp, 0, _create_svector_sexp(mat_iname, 
                "sdf_matrix_rownames", "name", "character"));
    SET_VECTOR_ELT(tmp, 1, duplicate(VECTOR_ELT(dimnames, 1)));
    setAttrib(ret, SDF_DimNamesSymbol, tmp);

    /* sdf.vector.type already set */

    UNPROTECT(i);
    UNUSE_SDF2(mat_iname);

    return ret;
}

SEXP sdf_get_matrix_columns(SEXP smat, SEXP cols) {
    SEXP colnames, ret;
    sqlite3_stmt *stmt1, *stmt2;
    const char *mat_iname, *coltype, *vectype;
    char *mat2_iname;
    int i, nrows, ncols, index, idxlen, ci_len, ci_actual_len;
    int *col_indices;

    mat_iname = SDF_INAME(smat);
    if (!USE_SDF1(mat_iname, TRUE, TRUE)) return R_NilValue;

    idxlen = LENGTH(cols);
    sprintf(g_sql_buf[0], "[%s].sdf_matrix_colnames", mat_iname);
    ncols = _get_row_count2(g_sql_buf[0], FALSE);
    sprintf(g_sql_buf[0], "[%s].sdf_matrix_rownames", mat_iname);
    nrows = _get_row_count2(g_sql_buf[0], FALSE);


    /* get column indices selected */
    ci_len = (ncols > idxlen) ? ncols : idxlen;
    col_indices = (int *)R_alloc(ci_len, sizeof(int));
    ci_actual_len = 0;

    if (IS_NUMERIC(cols)) {
        for (i = 0; i < idxlen; i++) {
            index = (int) REAL(cols)[i];
            if (index <= ncols && index > 0) col_indices[ci_actual_len++] = index-1;
        }
    } else if (IS_INTEGER(cols)) {
        for (i = 0; i < idxlen; i++) {
            index = INTEGER(cols)[i];
            if (index <= ncols && index > 0) col_indices[ci_actual_len++] = index-1;
        }
    } else if (IS_LOGICAL(cols)) {
        for (i = 0; i < ncols; i++) {
            if (LOGICAL(cols)[i % ncols]) col_indices[ci_actual_len++] = INTEGER(cols)[i];
        }
    }

    /* get matrix "mode" / type */
    sprintf(g_sql_buf[1], "select [row name], V1 from [%s].[sdf_data] limit ?,?", mat_iname);
    sqlite3_prepare(g_workspace, g_sql_buf[1], -1, &stmt1, NULL);
    sqlite3_bind_int(stmt1, 1, 1);  /* limit 1,1 */
    sqlite3_bind_int(stmt1, 2, 1);
    sqlite3_step(stmt1);
    coltype = (const char*) strcpy(g_sql_buf[0], sqlite3_column_decltype(stmt1, 1));
    sqlite3_finalize(stmt1);

    PROTECT(colnames = NEW_CHARACTER(ci_actual_len));

    /* create new matrix */
    mat2_iname = _create_svector1(mkString(mat_iname), coltype, NULL, TRUE);
    coltype = strcpy(g_sql_buf[3], g_sql_buf[0]);

    _sqlite_begin;

    /* create sdf_matrix_rownames and sdf_matrix_colnames */
    sprintf(g_sql_buf[0], "create table [%s].sdf_matrix_rownames(name text)", mat2_iname);
    _sqlite_error(_sqlite_exec(g_sql_buf[0]));
    sprintf(g_sql_buf[0], "create table [%s].sdf_matrix_colnames(name text)", mat2_iname);
    _sqlite_error(_sqlite_exec(g_sql_buf[0]));

    /* stmt to insert-select to the new matrix */
    sprintf(g_sql_buf[0], "insert into [%s].sdf_data([row name], V1) "
            " select [row name], V1 from [%s].[sdf_data] limit ?,?",
            mat2_iname, mat_iname);
    sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt1, NULL);

    /* stmt to insert-select to the new matrix-column table */
    sprintf(g_sql_buf[0], "insert into [%s].sdf_matrix_colnames "
            " select name from [%s].sdf_matrix_colnames limit ?, 1", mat2_iname, mat_iname);
    sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt2, NULL); 

    for (i = 0; i < ci_actual_len; i++) {
        sqlite3_reset(stmt1);
        sqlite3_bind_int(stmt1, 1, col_indices[i]*nrows);
        sqlite3_bind_int(stmt1, 2, nrows);
        sqlite3_step(stmt1);

        sqlite3_reset(stmt2);
        sqlite3_bind_int(stmt2, 1, col_indices[i]+1);
        sqlite3_step(stmt2);
    }

    sqlite3_finalize(stmt1);
    sqlite3_finalize(stmt2);

    /* copy colnames to a sexp, as the sexp's names() */
    sprintf(g_sql_buf[0], "select name from [%s].sdf_matrix_colnames", mat2_iname);
    sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt1, NULL);

    for (i = 0; sqlite3_step(stmt1) == SQLITE_ROW; i++) {
        SET_STRING_ELT(colnames, i, mkChar((char *)sqlite3_column_text(stmt1, 0)));
    }

    sqlite3_finalize(stmt1);

    /* copy matrix-rows table */
    sprintf(g_sql_buf[0], "insert into [%s].sdf_matrix_rownames "
            "select * from [%s].sdf_matrix_rownames", mat2_iname, mat_iname);
    _sqlite_error(_sqlite_exec(g_sql_buf[0]));

    _sqlite_commit;

    /* return matrix SEXP */
    if (strcmp(coltype, "text") == 0) vectype = "character";
    else if (strcmp(coltype, "int") == 0) vectype = "integer";
    else if (strcmp(coltype, "double") == 0) vectype = "numeric";
    ret = _create_smat_sexp(mat2_iname, coltype, nrows, ci_actual_len, colnames);

    

    UNPROTECT(1);
    UNUSE_SDF2(mat_iname);
    UNUSE_SDF2(mat2_iname);
    return ret;
}
