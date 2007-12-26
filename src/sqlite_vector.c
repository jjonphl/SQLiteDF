#include "sqlite_dataframe.h"
#include <math.h>
#include "Rmath.h"

/****************************************************************************
 * UTILITY FUNCTIONS
 ****************************************************************************/
char *_create_svector1(SEXP name, const char *type, int * _namelen, int protect) {
    int namelen, res;
    char *iname = _create_sdf_skeleton1(name, &namelen, protect);

    if (iname == NULL) return NULL;

    sprintf(g_sql_buf[2], "create table [%s].sdf_data ([row name] text, "
            "V1 %s, primary key ([row name]))", iname, type);
    res = _sqlite_exec(g_sql_buf[2]);
    _sqlite_error(res);

    if (_namelen != NULL) *_namelen = namelen;
    return iname;
}

SEXP _create_svector_sexp(const char *iname, const char *tblname,
        const char *varname, const char *type) {
    SEXP ret, value; int nprotected = 0;
    PROTECT(ret = NEW_LIST(3)); nprotected++;

    /* set list names */
    PROTECT(value = NEW_CHARACTER(3)); nprotected++;
    SET_STRING_ELT(value, 0, mkChar("iname"));
    SET_STRING_ELT(value, 1, mkChar("tblname"));
    SET_STRING_ELT(value, 2, mkChar("varname"));
    SET_NAMES(ret, value);

    /* set list values */
    SET_VECTOR_ELT(ret, 0, mkString(iname));
    SET_VECTOR_ELT(ret, 1, mkString(tblname));
    SET_VECTOR_ELT(ret, 2, mkString(varname));

    /* set sexp class */
    SET_CLASS(ret, mkString("sqlite.vector"));

    /* set sdf.vector.type */
    SET_SDFVECTORTYPE(ret, mkString(type));

    UNPROTECT(nprotected);

    return ret;
}

/* if ret == NULL, 3rd arg is the length of the vector to be created. otherwise
 * it is the index in the vector ret where we will put the result extracted
 * from ret */
int _get_vector_index_typed_result(sqlite3_stmt *stmt, SEXP *ret, int colidx,
        int idx_or_len, int *coltype) {
    int added = 1, ctype;
    if (*ret == NULL || *ret == R_NilValue) {
        if ((ctype = sqlite3_column_type(stmt, colidx)) == SQLITE_NULL) {
            added = 0;
        }
        
        if (ctype == SQLITE_TEXT) {
            PROTECT(*ret = NEW_CHARACTER(idx_or_len));
            if (added) 
                SET_STRING_ELT(*ret, 0, mkChar((char *)sqlite3_column_text(stmt, colidx)));
        } else if (ctype == SQLITE_FLOAT) {
            PROTECT(*ret = NEW_NUMERIC(idx_or_len));
            if (added) REAL(*ret)[0] = sqlite3_column_double(stmt, colidx);
        } else if (ctype == SQLITE_INTEGER) {
            /* sqlite does not differentiate b/w ints & bits, so we have to
             * do it ourselves since R distinguishes b/w ints & logicals */
            if (strcmp(sqlite3_column_decltype(stmt, colidx), "bit") == 0) {
                ctype = SQLITEDF_BIT; 
                PROTECT(*ret = NEW_LOGICAL(idx_or_len));
                if (added) LOGICAL(*ret)[0] = sqlite3_column_int(stmt, colidx);
            } else {
                PROTECT(*ret = NEW_INTEGER(idx_or_len));
                if (added) INTEGER(*ret)[0] = sqlite3_column_int(stmt, colidx);
            }
        } else added = 0;

        if (added) UNPROTECT(1);
        *coltype = ctype;
    } else if (stmt != NULL) {
        int ctype = *coltype;

        /* first row/previous rows must have been NULL so that *coltype 
         * was not set */
        if (ctype == SQLITE_NULL) {
            ctype = *coltype = sqlite3_column_type(stmt, colidx);
            /* distinguish int or logical */
            if (ctype == SQLITE_INTEGER) 
                ctype = (strcmp(sqlite3_column_decltype(stmt, colidx), "bit") == 0) ?
                    SQLITEDF_BIT : SQLITE_INTEGER;
        }

        if (sqlite3_column_type(stmt, colidx) == SQLITE_NULL) {
            added = 0;
        } else if (ctype == SQLITE_TEXT) {
            SET_STRING_ELT(*ret, idx_or_len, 
                    mkChar((char *)sqlite3_column_text(stmt, colidx)));
        } else if (ctype == SQLITE_FLOAT) {
            REAL(*ret)[idx_or_len] = sqlite3_column_double(stmt, colidx);
        } else if (ctype == SQLITE_INTEGER) {
            INTEGER(*ret)[idx_or_len] = sqlite3_column_int(stmt, colidx);
        } else if (ctype == SQLITEDF_BIT) {
            LOGICAL(*ret)[idx_or_len] = sqlite3_column_int(stmt, colidx);
        } else added = 0;
    } else if (stmt == NULL) {
        /* used when there is no row returned (sqlite3_step(stmt)!= QLITE_ROW),
         * and we just insert NAs in the result */
        ctype = *coltype;
        if (ctype == SQLITE_NULL) { 
            /* since stmt == NULL, we can't check its column type */
            added = 0;
        } else if (ctype == SQLITE_TEXT) {
            SET_STRING_ELT(*ret, idx_or_len, NA_STRING);
        } else if (ctype == SQLITE_FLOAT) {
            REAL(*ret)[idx_or_len] = NA_REAL;
        } else if (ctype == SQLITE_INTEGER) {
            INTEGER(*ret)[idx_or_len] = NA_INTEGER;
        } else if (ctype == SQLITEDF_BIT) {
            LOGICAL(*ret)[idx_or_len] = NA_LOGICAL;
        } else added = 0;
    }
        
    return added;
}

/****************************************************************************
 * SVEC FUNCTIONS
 ****************************************************************************/
SEXP sdf_get_variable(SEXP sdf, SEXP name) {
    const char *iname, *varname, *svec_type = NULL;
    const char *coltype;
    int type = -1, res, nprotected = 0;
    SEXP ret, value;

    if (!IS_CHARACTER(name)) {
        error("argument is not a string.\n");
    }

    iname = SDF_INAME(sdf);
    varname = CHAR_ELT(name, 0);

    if (!USE_SDF1(iname, TRUE, FALSE)) return R_NilValue;

    /* check if sdf & varname w/in that sdf exists */
    sqlite3_stmt *stmt;
    sprintf(g_sql_buf[0], "select [%s] from [%s].sdf_data", varname, iname);

    res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0);

    if (_sqlite_error(res)) return R_NilValue;

    coltype = sqlite3_column_decltype(stmt, 0);
    sqlite3_finalize(stmt);
    
    PROTECT(ret = NEW_LIST(3)); nprotected++;

    /* set list names */
    PROTECT(value = NEW_CHARACTER(3)); nprotected++;
    SET_STRING_ELT(value, 0, mkChar("iname"));
    SET_STRING_ELT(value, 1, mkChar("tblname"));
    SET_STRING_ELT(value, 2, mkChar("varname"));
    SET_NAMES(ret, value);

    /* set list values */
    SET_VECTOR_ELT(ret, 0, mkString(iname));
    SET_VECTOR_ELT(ret, 1, mkString("sdf_data"));
    SET_VECTOR_ELT(ret, 2, mkString(varname));

    /* set class */
    if (strcmp(coltype, "text") == 0) svec_type = "character";
    else if (strcmp(coltype, "double") == 0) svec_type = "numeric";
    else if (strcmp(coltype, "bit") == 0) svec_type = "logical";
    else if (strcmp(coltype, "integer") == 0 || strcmp(coltype, "int") == 0) {
        /* determine if int, factor or ordered */
        type = _get_factor_levels1(iname, varname, ret, FALSE);
        switch(type) {
            case VAR_INTEGER: svec_type = "integer"; break;
            case VAR_FACTOR: svec_type = "factor"; break;
            case VAR_ORDERED: svec_type = "ordered";
        }
    }

    SET_CLASS(ret, mkString("sqlite.vector"));
    SET_SDFVECTORTYPE(ret, mkString(svec_type));

    UNPROTECT(nprotected);
    return ret;

}


SEXP sdf_get_variable_length(SEXP svec) {
    const char *iname = SDF_INAME(svec);
    if (!USE_SDF1(iname, TRUE, FALSE)) return R_NilValue;

    return ScalarInteger(_get_row_count2(iname, 1));
}

    
SEXP sdf_get_variable_index(SEXP svec, SEXP idx) {
    SEXP ret = R_NilValue, tmp;
    const char *iname = SDF_INAME(svec), *tblname = SVEC_TBLNAME(svec),
               *varname = SVEC_VARNAME(svec);
    int *index, _idx, nrows, i, init=FALSE, retlen=0, res, coltype;
    sqlite3_stmt *stmt;

    if (!USE_SDF1(iname, TRUE, FALSE)) return R_NilValue;

    /* get # of rows in svec */
    nrows = _get_row_count2(iname, TRUE);
    if (nrows < 1) return ret;

    sprintf(g_sql_buf[0], "select [%s] from [%s].[%s] where rowid=?",
            varname, iname, tblname);
    res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0);
    if (_sqlite_error(res)) error("cannot complete request");

    index = _make_row_index(idx, &nrows);

    for (i = 0; i < nrows; i++) {
        _idx = index[i];
        if (_idx < 1 || _idx == NA_INTEGER) continue;
        sqlite3_reset(stmt);
        sqlite3_bind_int(stmt, 1, _idx);
        res = sqlite3_step(stmt);
        if (!init) {
            if (res == SQLITE_ROW) { 
                /* valid 1st row. initialize the returned vector with length
                 * nrows. retlen holds the length of vector ret. 
                 */ 
                retlen = _get_vector_index_typed_result(stmt, &ret, 0, nrows, &coltype);
            } else {
                /* invalid 1st row, initalize anyway but now with length 
                 * nrows-1. we put dummy results in 1st element of vector which
                 * should be overwritten in succeeding calls
                 */
                sqlite3_reset(stmt);
                sqlite3_bind_int(stmt, 1, 0);  /* get 1st row */
                res = sqlite3_step(stmt);
                _get_vector_index_typed_result(stmt, &ret, 0, nrows - 1, &coltype);
                retlen = 0;
            }
            init = TRUE;
        } else {
            retlen += _get_vector_index_typed_result((res == SQLITE_ROW) ? stmt : NULL,
                            &ret, 0, retlen, &coltype);
        }
    }

    sqlite3_finalize(stmt);

    if (ret != R_NilValue) {
        ret = _shrink_vector(ret, retlen);
        tmp = GET_LEVELS(svec);
        if (tmp != R_NilValue) {
            SET_LEVELS(ret, duplicate(tmp));
            if (TEST_SDFVECTORTYPE(svec, "factor")) {
                SET_CLASS(ret, mkString("factor"));
            } else {
                PROTECT(tmp = NEW_CHARACTER(2));
                SET_STRING_ELT(tmp, 0, mkChar("ordered"));
                SET_STRING_ELT(tmp, 1, mkChar("factor"));
                UNPROTECT(1);
            }
        }
    }

    return ret;
}

/* sqlite.vector.[<- */
SEXP sdf_set_variable_index(SEXP svec, SEXP idx, SEXP value) {
    int *index;
    int coltype, valtype, idx_len, val_len, svec_len, i, i2, res;
    const char *iname = SDF_INAME(svec), *tblname = SVEC_TBLNAME(svec),
               *varname = SVEC_VARNAME(svec);
    const char *decltype;
    sqlite3_stmt *stmt;
    char *error_msg = NULL;


    if (!USE_SDF1(iname, TRUE, FALSE)) return R_NilValue;

    /* find the type of the svec */
    sprintf(g_sql_buf[0], "select [%s] from [%s].[%s] where not [%s] is null limit 1", 
                varname, iname, tblname, varname);
    res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0);
    if (_sqlite_error(res)) error("cannot complete request");
    sqlite3_step(stmt);
    decltype = sqlite3_column_decltype(stmt, 0);
    //Rprintf("decltype: %s\n", decltype); 
    coltype = _get_r_type(decltype);
    sqlite3_finalize(stmt);

    /* get index */
    val_len = LENGTH(value);
    idx_len = svec_len = _get_row_count2(iname, TRUE);
    /*Rprintf("idx_len: %d\n", idx_len);*/
    index = _make_row_index(idx, &idx_len);
    if (idx_len % val_len != 0) 
        /* if val_len > idx_len, idx_len % val_len = idx_len > 0 */
        /* if idx_len > val_len, recycle if idx_len % val_len == 0 */
        warning("number of items to replace is not a multiple of replacement length");

    /* find the type of value to be assigned */
    valtype = TYPEOF(value);

    /* we will use the style similar to R's */
    coltype = coltype * 100 + valtype;

    sprintf(g_sql_buf[0], "update [%s].[%s] set [%s]=? where rowid=?",
            iname, tblname, varname);
    _sqlite_begin;
    res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0);
    if (_sqlite_error(res)) error("cannot complete request");
    /*Rprintf("idx_len: %d\n", idx_len);*/

    /*Rprintf("coltype: %d\n", coltype);*/
    switch (coltype) {
        case 1010: /* logical <- logical */
        case 1310: /* int <- logical */
        case 1313: /* int <- int */
            /* TODO: check if factor */
            for (i = 0; i < idx_len; i++) { 
                sqlite3_reset(stmt);
                i2 = i % val_len;
                sqlite3_bind_int(stmt, 1, INTEGER(value)[i2]);
                sqlite3_bind_int(stmt, 2, index[i]);
                sqlite3_step(stmt);
            }
            break;
        case 1410: /* real <- logical */
        case 1413: /* real <- int */
            for (i = 0; i < idx_len; i++) { 
                sqlite3_reset(stmt);
                i2 = i % val_len;
                sqlite3_bind_double(stmt, 1, (double) INTEGER(value)[i2]);
                sqlite3_bind_int(stmt, 2, index[i]);
                sqlite3_step(stmt);
            }
            break;
        case 1414: /* real <- real */
            for (i = 0; i < idx_len; i++) { 
                sqlite3_reset(stmt);
                i2 = i % val_len;
                sqlite3_bind_double(stmt, 1, REAL(value)[i2]);
                sqlite3_bind_int(stmt, 2, index[i]);
                res = sqlite3_step(stmt);
            }
            break;
        case 1610: /* character <- logical */
            for (i = 0; i < idx_len; i++) { 
                sqlite3_reset(stmt);
                i2 = i % val_len;
                sqlite3_bind_text(stmt, 1, 
                        (INTEGER(value)[i2]) ? "TRUE" : "FALSE", -1,
                        SQLITE_STATIC);
                sqlite3_bind_int(stmt, 2, index[i]);
                sqlite3_step(stmt);
            }
            break;
        case 1613: /* character <- integer */
            /* value could be a factor! */
            for (i = 0; i < idx_len; i++) { 
                sqlite3_reset(stmt);
                i2 = i % val_len;
                res = sprintf(g_sql_buf[1], "%d", INTEGER(value)[i2], 
                        SQLITE_STATIC);
                sqlite3_bind_text(stmt, 1, g_sql_buf[1], res, SQLITE_STATIC);
                sqlite3_bind_int(stmt, 2, index[i]);
                sqlite3_step(stmt);
            }
            break;
        case 1614: { /* character <- real */
            SEXP chvalue;
            PROTECT(chvalue = AS_CHARACTER(value));
            for (i = 0; i < idx_len; i++) { 
                sqlite3_reset(stmt);
                i2 = i % val_len;
                sqlite3_bind_text(stmt, 1, CHAR(STRING_ELT(chvalue, i2)), 
                        -1, SQLITE_STATIC);
                sqlite3_bind_int(stmt, 2, index[i]);
                sqlite3_step(stmt);
            }
            UNPROTECT(1);
          }
            break;
        case 1616: /* character <- character */
            for (i = 0; i < idx_len; i++) { 
                sqlite3_reset(stmt);
                i2 = i % val_len;
                sqlite3_bind_text(stmt, 1, CHAR(STRING_ELT(value, i2)), 
                        -1, SQLITE_STATIC);
                sqlite3_bind_int(stmt, 2, index[i]);
                sqlite3_step(stmt);
            }
            break;
        case 1314: /* int <- real */
            error_msg = "cannot promote vector from integer to real";
            break;
        case 1014: /* logical <- real */
            error_msg = "cannot promote vector from logical to real";
            break;
        case 1013: /* logical <- int */
            error_msg = "cannot promote vector from logical to integer";
            break;
        case 1016: /* logical <- character */
            error_msg = "cannot promote vector from logical to character";
            break;
        case 1316: /* integer <- character */
            /* TODO: this could be factors */
            error_msg = "cannot promote vector from integer to character";
            break;
        case 1416: /* real <- character */
            error_msg = "cannot promote vector from real to character";
            break;
        default:
            sprintf(g_sql_buf[0], "don't know what to do with coltype=%d", 
                    coltype);
            error_msg = g_sql_buf[0];
    }
    _sqlite_commit;
    if (error_msg != NULL) error(error_msg);

    return svec;
}

SEXP sdf_variable_summary(SEXP svec, SEXP maxsum) {
    const char *iname, *tblname, *varname, *type;
    sqlite3_stmt *stmt;
    int nprotected = 0;
    SEXP ret, names;

    iname = SDF_INAME(svec);
    tblname = SVEC_TBLNAME(svec);
    varname = SVEC_VARNAME(svec);
    USE_SDF1(iname, TRUE, FALSE);

    if ((TEST_SDFVECTORTYPE(svec, "ordered") && ((type = "ordered"))) ||
            (TEST_SDFVECTORTYPE(svec, "factor") && ((type = "factor")))) {
        int nrows, i, max_rows = INTEGER(maxsum)[0];


        sprintf(g_sql_buf[0], "[%s].[%s %s]", iname, type, varname);
        nrows = _get_row_count2(g_sql_buf[0], FALSE);
        if (nrows <= max_rows) max_rows = nrows;
        else { nrows = max_rows; max_rows--; }

        PROTECT(ret = NEW_INTEGER(nrows)); nprotected = 1;
        PROTECT(names = NEW_CHARACTER(nrows)); nprotected++;

        sprintf(g_sql_buf[0], "select [%s].[%s %s].label, count(*) from "
                "[%s].[%s] join [%s].[%s %s] on [%s].[%s].[%s]=[%s].[%s %s].level "
                "group by [%s].[%s].[%s], [%s].[%s %s].level order by count(*) desc",
                iname, type, varname, iname, tblname, iname, type, varname, iname, tblname, varname,
                iname, type, varname, iname, tblname, varname, iname, type, varname);
        sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0);

        for (i = 0; i < max_rows; i++) {
            sqlite3_step(stmt);
            SET_STRING_ELT(names, i, mkChar((char *)sqlite3_column_text(stmt, 0)));
            INTEGER(ret)[i] = sqlite3_column_int(stmt, 1);
        }

        if (nrows > max_rows) {
            int others_sum = 0;
            SET_STRING_ELT(names, nrows-1, mkChar("(Others)"));
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                others_sum += sqlite3_column_int(stmt, 1);
            }
            INTEGER(ret)[nrows-1] = others_sum;
        }
    } else if (TEST_SDFVECTORTYPE(svec, "logical")) {
        sprintf(g_sql_buf[0], "select count(*) from "
                "[%s].[%s] group by [%s] order by [%s]", iname, tblname, varname, varname);

        PROTECT(names = NEW_CHARACTER(3)); nprotected = 1;
        PROTECT(ret = NEW_CHARACTER(3)); nprotected++;

        SET_STRING_ELT(names, 0, mkChar("Mode"));
        SET_STRING_ELT(names, 1, mkChar("FALSE"));
        SET_STRING_ELT(names, 2, mkChar("TRUE"));

        sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0);
        SET_STRING_ELT(ret, 0, mkChar("logical"));
        sqlite3_step(stmt);
        SET_STRING_ELT(ret, 1, mkChar((const char *)sqlite3_column_text(stmt, 0)));
        sqlite3_step(stmt);
        SET_STRING_ELT(ret, 2, mkChar((const char *)sqlite3_column_text(stmt, 0)));

    } else return R_NilValue;

    sqlite3_finalize(stmt);
    SET_NAMES(ret, names);
    SET_CLASS(ret, mkString("table"));
    UNPROTECT(nprotected);
    return ret;
}
            

/* the global accumulator should be safe if we only do 1 cummulative or
 * aggregate at any time. it won't work for stuffs like "select max(col)-min(col)
 * from sdf_data". */
static long double g_accumulator = 0.0; /* accumulator var for cumsum, cumprod, etc. */
static int g_start = 0;            /* flag for start of cummulation */
static int g_narm = 0;             /* for Summary group */

void _init_sqlite_function_accumulator() {
    g_accumulator = 0.0;   /* initialize accumulator */
    g_start = 1;           /* flag that we are at start of accumulating */
}

SEXP sdf_do_variable_math(SEXP func, SEXP vector, SEXP other_args) {
    const char *iname_src, *varname_src, *funcname;
    char *iname;
    int namelen, res;
    sqlite3_stmt *stmt;

    /* get data from arguments (function name and sqlite.vector stuffs) */
    funcname = CHAR_ELT(func, 0);
    iname_src = SDF_INAME(vector);
    varname_src = SVEC_VARNAME(vector);

    if (!USE_SDF1(iname_src, TRUE, TRUE)) return R_NilValue;

    /* create a new sdf, with 1 column named V1 */
    iname = _create_svector1(R_NilValue, "double", &namelen, TRUE);

    /* insert into <newsdf>.col, row.names select func(col), rownames */
    if (strcmp(funcname, "round") == 0 || strcmp(funcname, "signif") == 0) {
        double digits = REAL(_getListElement(other_args, "digits"))[0];
        sprintf(g_sql_buf[0], "insert into [%s].sdf_data([row name], V1) "
                "select [row name], %s([%s],?) from [%s].sdf_data", iname, funcname,
                varname_src, iname_src);
        res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0); 
        if (_sqlite_error(res)) goto vecmath_prepare_error;
        res = sqlite3_bind_double(stmt, 1, digits);
    } else if (strcmp(funcname, "log") == 0) {
        double base = REAL(_getListElement(other_args, "base"))[0];
        sprintf(g_sql_buf[0], "insert into [%s].sdf_data([row name], V1) "
                "select [row name], %s([%s],?) from [%s].sdf_data", iname, funcname,
                varname_src, iname_src);
        res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0); 
        if (_sqlite_error(res)) goto vecmath_prepare_error;
        res = sqlite3_bind_double(stmt, 1, base);
    } else {
        sprintf(g_sql_buf[0], "insert into [%s].sdf_data([row name], V1) "
                "select [row name], %s([%s]) from [%s].sdf_data", iname, funcname,
                varname_src, iname_src);
        res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0); 
    }

    if (_sqlite_error(res)) {
vecmath_prepare_error:
        sprintf(g_sql_buf[0], "detach %s", iname);
        _sqlite_exec(g_sql_buf[0]);

        /* we will return a string with the file name, and do file.remove
         * at R */
        iname[namelen] = '.';
        return mkString(iname);
    }

    _init_sqlite_function_accumulator();
    sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    UNUSE_SDF2(iname);
    UNUSE_SDF2(iname_src);

    return _create_svector_sexp(iname, "sdf_data", "V1", "numeric");
}

SEXP sdf_do_variable_op(SEXP func, SEXP vector, SEXP op2, SEXP arg_reversed) {
    const char *iname_src, *varname_src, *funcname;
    char *iname = NULL; 
    int res, functype = -1, op2_len, svec_len, i, reversed;
    sqlite3_stmt *stmt, *stmt2;

    /* get data from arguments (function name and sqlite.vector stuffs) */
    funcname = CHAR_ELT(func, 0);
    iname_src = SDF_INAME(vector);
    varname_src = SVEC_VARNAME(vector);
    reversed = LOGICAL(arg_reversed)[0];

    if (!USE_SDF1(iname_src, TRUE, TRUE)) return R_NilValue;
    switch(funcname[0]) {
        case '+' :
        case '-' :
        case '*' :
        case '/' :
        case '^' :
        case '%' : /* %% and %/% */ 
            functype = 0; break;  /* output is REAL */
        case '&' :
        case '|' :
        case '!' :  /* ! and != */
            if (funcname[1] == 0) { functype = 1; break; }
        case '=' :  /* == */
        case '<' :  /* < and <= */
        case '>' :  /* > and >= */
            functype = 2; 
    }

    svec_len = _get_row_count2(iname_src, 1);
    if (functype == 0 || functype == 2 || (functype == 1 && funcname[0] != '!')) {
        char *insert_fmt_string1c, *insert_fmt_string1s;
        char *insert_fmt_string2c, *insert_fmt_string2s;
        int vec_idx, sdf_idx;

        iname = _create_svector1(R_NilValue, (functype == 0) ? "double" : "bit", NULL, TRUE);

        /* insert_fmt_string prefixes:
         * 1 - op2 is an ordinary vector, op2_len = 1, straightforward insert-select
         * 2 - op2 is an ordinary vector, op2_len > 1, may need recycling, loop both sides
         * s - operator will be printed as string
         * c - operator is either INTDIV or RMOD (int division, R modulo). initially, I
         *     thought I could cast both op to int then use / and % of sqlite, which is just
         *     the 2nd character of the funcname. however, R's INTDIV and casts to int 
         *     after division (e.g. 3.5 %/% 1.5 == 2). RMOD operates on doubles too,
         *     which is the remainder of the largest int multiple of "divisor"
         *     (e.g. 3.5 %% 1.5 = 0.5)
         */ 
        if (functype == 1) { /* boolean binary operators */
            if (!reversed) {
                insert_fmt_string1s = "insert into [%s].sdf_data "
                            "select [row name], ([%s] != 0) %s (? != 0) from [%s].sdf_data";
            } else {
                insert_fmt_string1s = "insert into [%s].sdf_data "
                            "select [row name], (? != 0) %s ([%s] != 0) from [%s].sdf_data";
            }
            insert_fmt_string2s = "insert into [%s].sdf_data values(?, (? != 0) %s (? != 0))";
            /* no need for char version of fmt_string2, since %% only occurs for functype==0 */
            insert_fmt_string1c = insert_fmt_string1s;
            insert_fmt_string2c = insert_fmt_string2s;
        } else {
            if (!reversed) {
                insert_fmt_string1s = "insert into [%s].sdf_data "
                            "select [row name], [%s] %s ? from [%s].sdf_data";
                insert_fmt_string1c = "insert into [%s].sdf_data "
                            "select [row name], %s([%s],?) from [%s].sdf_data";
            } else {
                insert_fmt_string1s = "insert into [%s].sdf_data "
                            "select [row name], ? %s [%s] from [%s].sdf_data";
                insert_fmt_string1c = "insert into [%s].sdf_data "
                            "select [row name], %s(?,[%s]) from [%s].sdf_data";
            }
            /* fmt_string2c format operator from a char, used for %% and %/% */
            insert_fmt_string2c = "insert into [%s].sdf_data values(?, %s(?,?))";
            insert_fmt_string2s = "insert into [%s].sdf_data values(?, ? %s ?)";
        }

        /* for 2* insert statements, the values below specifies the index of bind().
         * if !reversed, then e1 is svec, e2 is anything. otherwise, e2 is the svec */
        if (reversed) { sdf_idx = 3; vec_idx = 2; }
        else { sdf_idx = 2; vec_idx = 3; }

        if (IS_NUMERIC(op2)) {
            op2_len = LENGTH(op2);

            if (op2_len == 1) {
                if (funcname[0] == '%') {
                    sprintf(g_sql_buf[2], insert_fmt_string1c, iname, 
                            (funcname[1] == '%') ? "r_mod" : "r_intdiv", varname_src, iname_src);
                } else {
                    sprintf(g_sql_buf[2], insert_fmt_string1s, iname,
                            varname_src, funcname, iname_src);
                }
                res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                _sqlite_error(res);
                sqlite3_bind_double(stmt, 1, REAL(op2)[0]);
                sqlite3_step(stmt);
                sqlite3_finalize(stmt);
            } else if (op2_len <= svec_len) {  /* recycle op2 */
                sprintf(g_sql_buf[2], "select [row name], [%s] from [%s].sdf_data",
                        varname_src, iname_src);
                res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt2, 0);
                _sqlite_error(res);

                if (funcname[0] == '%') {
                    sprintf(g_sql_buf[2], insert_fmt_string2c, iname, 
                            (funcname[1] == '%') ? "r_mod" : "r_intdiv");  
                    _sqlite_begin;
                    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                    _sqlite_error(res);

                    for (i = 0; i < svec_len; i++) {
                        sqlite3_step(stmt2);

                        sqlite3_reset(stmt);
                        sqlite3_bind_text(stmt, 1, (char *)sqlite3_column_text(stmt2, 0), -1, SQLITE_STATIC);
                        sqlite3_bind_int(stmt, sdf_idx, (int)sqlite3_column_double(stmt2, 1));
                        sqlite3_bind_int(stmt, vec_idx, (int)REAL(op2)[i % op2_len]);
                        sqlite3_step(stmt);
                    }
                    sqlite3_finalize(stmt);
                    sqlite3_finalize(stmt2);
                    _sqlite_commit;
                } else { /* non-integer binary operation */
                    sprintf(g_sql_buf[2], insert_fmt_string2s, iname, funcname);
                    _sqlite_begin;
                    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                    _sqlite_error(res);

                    for (i = 0; i < svec_len; i++) {
                        sqlite3_step(stmt2);

                        sqlite3_reset(stmt);
                        sqlite3_bind_text(stmt, 1, (char *)sqlite3_column_text(stmt2, 0), -1, SQLITE_STATIC);
                        sqlite3_bind_double(stmt, sdf_idx, sqlite3_column_double(stmt2, 1));
                        sqlite3_bind_double(stmt, vec_idx, REAL(op2)[i % op2_len]);
                        sqlite3_step(stmt);
                    }
                    sqlite3_finalize(stmt);
                    sqlite3_finalize(stmt2);
                    _sqlite_commit;
                }
            } else { /* op2_len > svec_len, recycle svec */
                sprintf(g_sql_buf[1], "select [%s] from [%s].sdf_data",
                        varname_src, iname_src);

                if (funcname[0] == '%') {
                    sprintf(g_sql_buf[2], insert_fmt_string2c, iname, 
                            (funcname[1] == '%') ? "r_mod" : "r_intdiv");
                    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                    _sqlite_error(res);
                } else {
                    sprintf(g_sql_buf[2], insert_fmt_string2s, iname, funcname);
                    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                    _sqlite_error(res);
                }

                i = 0; _sqlite_begin;
                while (i < op2_len) {
                    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt2, 0);
                    _sqlite_error(res);

                    if (funcname[0] == '%') {
                        for ( ; i < op2_len && sqlite3_step(stmt) == SQLITE_ROW ; i++) {
                            sqlite3_step(stmt2);

                            sqlite3_reset(stmt);
                            /* simplify my life, just use ints for row names so that we
                             * don't have to worry about duplicates */
                            sqlite3_bind_int(stmt, 1, i); 
                            sqlite3_bind_int(stmt, sdf_idx, sqlite3_column_int(stmt2, 1));
                            sqlite3_bind_int(stmt, vec_idx, (int)REAL(op2)[i % op2_len]);
                            sqlite3_step(stmt);
                        }
                    } else {
                        for ( ; i < op2_len && sqlite3_step(stmt) == SQLITE_ROW ; i++) {
                            sqlite3_step(stmt2);

                            sqlite3_reset(stmt);
                            sqlite3_bind_int(stmt, 1, i);
                            sqlite3_bind_double(stmt, sdf_idx, sqlite3_column_double(stmt2, 1));
                            sqlite3_bind_double(stmt, vec_idx, REAL(op2)[i % op2_len]);
                            sqlite3_step(stmt);
                        }
                    }

                    /* recycle on svec if we loop again */
                    sqlite3_finalize(stmt2);
                }

                sqlite3_finalize(stmt);
                _sqlite_commit;
            }

        } else if (IS_INTEGER(op2) || IS_LOGICAL(op2)) { /* I N T E G E R */
            /* we are taking advantage of the fact that logicals are stored as int.
             * if this becomes untrue in the future, then this is a bug */
            /* we are binding double because ___ (?) */
            op2_len = LENGTH(op2);

            if (op2_len == 1) {
                if (funcname[0] == '%') {
                    sprintf(g_sql_buf[2], insert_fmt_string1c, iname, 
                            varname_src, (funcname[1] == '%') ? "r_mod" : "r_intdiv", iname_src);
                } else {
                    sprintf(g_sql_buf[2], insert_fmt_string1s, iname,
                            varname_src, funcname, iname_src);
                }
                res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                _sqlite_error(res);
                sqlite3_bind_double(stmt, 1, (double)INTEGER(op2)[0]);
                sqlite3_step(stmt);
            } else if (op2_len <= svec_len) {
                sprintf(g_sql_buf[2], "select [row name], [%s] from [%s].sdf_data",
                        varname_src, iname_src);
                res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt2, 0);
                _sqlite_error(res);

                if (funcname[0] == '%') {
                    sprintf(g_sql_buf[2], insert_fmt_string2c, iname, 
                            (funcname[1] == '%') ? "r_mod" : "r_intdiv");
                    _sqlite_begin;
                    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                    _sqlite_error(res);

                    for (i = 0; i < svec_len; i++) {
                        sqlite3_step(stmt2);

                        sqlite3_reset(stmt);
                        sqlite3_bind_text(stmt, 1, (char *)sqlite3_column_text(stmt2, 0), -1, SQLITE_STATIC);
                        sqlite3_bind_int(stmt, sdf_idx, sqlite3_column_int(stmt2, 1));
                        sqlite3_bind_int(stmt, vec_idx, INTEGER(op2)[i % op2_len]);
                        sqlite3_step(stmt);
                    }
                    sqlite3_finalize(stmt);
                    sqlite3_finalize(stmt2);
                    _sqlite_commit;
                } else {
                    sprintf(g_sql_buf[2], insert_fmt_string2s, iname, funcname);
                    _sqlite_begin;
                    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                    _sqlite_error(res);

                    for (i = 0; i < svec_len; i++) {
                        sqlite3_step(stmt2);

                        sqlite3_reset(stmt);
                        sqlite3_bind_text(stmt, 1, (char *)sqlite3_column_text(stmt2, 0), -1, SQLITE_STATIC);
                        sqlite3_bind_double(stmt, sdf_idx, sqlite3_column_double(stmt2, 1));
                        sqlite3_bind_double(stmt, vec_idx, (double)INTEGER(op2)[i % op2_len]);
                        sqlite3_step(stmt);
                    }
                    sqlite3_finalize(stmt);
                    sqlite3_finalize(stmt2);
                    _sqlite_commit;
                }
            } else {
                sprintf(g_sql_buf[1], "select [%s] from [%s].sdf_data",
                        varname_src, iname_src);

                if (funcname[0] == '%') {
                    sprintf(g_sql_buf[2], insert_fmt_string2c, iname,
                            (funcname[1] == '%') ? "r_mod" : "r_intdiv");
                    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                    _sqlite_error(res);
                } else {
                    sprintf(g_sql_buf[2], insert_fmt_string2s, iname, funcname);
                    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                    _sqlite_error(res);
                }
               
                i = 0; _sqlite_begin; 
                while (i < op2_len) {
                    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt2, 0);
                    _sqlite_error(res);

                    if (funcname[0] == '%') {
                        for ( ; i < op2_len && sqlite3_step(stmt) == SQLITE_ROW ; i++) {
                            sqlite3_step(stmt2);

                            sqlite3_reset(stmt);
                            /* simplify my life, just use ints for row names so that we
                             * don't have to worry about duplicates */
                            sqlite3_bind_int(stmt, 1, i); 
                            sqlite3_bind_int(stmt, sdf_idx, sqlite3_column_int(stmt2, 1));
                            sqlite3_bind_int(stmt, vec_idx, INTEGER(op2)[i % op2_len]);
                            sqlite3_step(stmt);
                        }
                    } else {
                        for ( ; i < op2_len && sqlite3_step(stmt) == SQLITE_ROW ; i++) {
                            sqlite3_step(stmt2);

                            sqlite3_reset(stmt);
                            sqlite3_bind_int(stmt, 1, i);
                            sqlite3_bind_double(stmt, sdf_idx, sqlite3_column_double(stmt2, 1));
                            sqlite3_bind_double(stmt, vec_idx, (double)INTEGER(op2)[i % op2_len]);
                            sqlite3_step(stmt);
                        }
                    }

                    /* recycle on svec if we loop again */
                    sqlite3_finalize(stmt2);
                }
                sqlite3_finalize(stmt);
                _sqlite_commit;
            }

        } else if (IS_CHARACTER(op2)) {
            if (functype != 2) error("not supported");

            op2_len = LENGTH(op2);

            if (op2_len == 1) {
                sprintf(g_sql_buf[2], insert_fmt_string1s, iname,
                            varname_src, funcname, iname_src);
                res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                _sqlite_error(res);
                sqlite3_bind_text(stmt, 1, CHAR_ELT(op2, 0), -1, SQLITE_STATIC);
                sqlite3_step(stmt);
            } else if (op2_len <= svec_len) {
                sprintf(g_sql_buf[2], "select [row name], [%s] from [%s].sdf_data",
                        varname_src, iname_src);
                res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt2, 0);
                _sqlite_error(res);

                sprintf(g_sql_buf[2], insert_fmt_string2s, iname, funcname);
                _sqlite_begin;
                res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                _sqlite_error(res);

                for (i = 0; i < svec_len; i++) {
                    sqlite3_step(stmt2);

                    sqlite3_reset(stmt);
                    sqlite3_bind_text(stmt, 1, (char *)sqlite3_column_text(stmt2, 0), -1, SQLITE_STATIC);
                    sqlite3_bind_text(stmt, sdf_idx, (char *)sqlite3_column_text(stmt2, 1), -1, SQLITE_STATIC);
                    sqlite3_bind_text(stmt, vec_idx, CHAR_ELT(op2, i % op2_len), -1 , SQLITE_STATIC);
                    sqlite3_step(stmt);
                }
                sqlite3_finalize(stmt2);
                _sqlite_commit;
            } else {
                sprintf(g_sql_buf[1], "select [%s] from [%s].sdf_data",
                        varname_src, iname_src);

                sprintf(g_sql_buf[2], insert_fmt_string2s, iname, funcname);
                res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                _sqlite_error(res);
               
                i = 0; _sqlite_begin; 
                while (i < op2_len) {
                    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt2, 0);
                    _sqlite_error(res);

                    for ( ; i < op2_len && sqlite3_step(stmt) == SQLITE_ROW ; i++) {
                        sqlite3_step(stmt2);

                        sqlite3_reset(stmt);
                        sqlite3_bind_int(stmt, 1, i);
                        sqlite3_bind_text(stmt, sdf_idx, (char *)sqlite3_column_text(stmt2, 1), -1, SQLITE_STATIC);
                        sqlite3_bind_text(stmt, vec_idx, CHAR_ELT(op2, i % op2_len), -1 , SQLITE_STATIC);
                        sqlite3_step(stmt);
                    }

                    /* recycle on svec if we loop again */
                    sqlite3_finalize(stmt2);
                }
                sqlite3_finalize(stmt);
                _sqlite_commit;
            }
        
        } else if (inherits(op2, "sqlite.vector")) { 
            /* op2 is surely not a factor, as handled by the R wrapper */
            /* even though it is impossible for reversed to be FALSE, still use
             * sdf_idx and vec_idx so that code would be less confusing */
            const char *iname_op2, *varname_op2;
            sqlite3_stmt *stmt3;
            iname_op2 = SDF_INAME(op2);
            varname_op2 = SVEC_VARNAME(op2);

            if (!USE_SDF1(iname_op2, TRUE, TRUE)) {
                /* delete created sqlite.vector */
                warning("detaching created result SDF %s\n", iname);
                sdf_detach_sdf(mkString(iname));
                return R_NilValue;
            }
            _sqlite_begin;

            op2_len = _get_row_count2(iname_op2, 1);
            
            sprintf(g_sql_buf[2], "select [%s] from [%s].sdf_data", varname_src, iname_src);
            res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt2, 0);
            _sqlite_error(res);

            sprintf(g_sql_buf[2], "select [%s] from [%s].sdf_data", varname_op2, iname_op2);
            res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt3, 0);
            _sqlite_error(res);

            if (funcname[0] == '%') {
                sprintf(g_sql_buf[2], insert_fmt_string2c, iname,
                            (funcname[1] == '%') ? "r_mod" : "r_intdiv");
                res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                _sqlite_error(res);

                if (svec_len == op2_len) {
                    for (i = 0; i < svec_len; i++) {
                        sqlite3_step(stmt2); sqlite3_step(stmt3);

                        sqlite3_reset(stmt);
                        sqlite3_bind_int(stmt, 1, i);
                        sqlite3_bind_int(stmt, sdf_idx, sqlite3_column_int(stmt2, 0));
                        sqlite3_bind_int(stmt, vec_idx, sqlite3_column_int(stmt3, 0));
                        sqlite3_step(stmt);
                    }
                } else if (svec_len < op2_len) {
                    i = 0;
                    while (i < op2_len) {
                        for ( ; sqlite3_step(stmt2) == SQLITE_ROW && i < op2_len; i++) {
                            sqlite3_step(stmt3);

                            sqlite3_reset(stmt);
                            sqlite3_bind_int(stmt, 1, i);
                            sqlite3_bind_int(stmt, sdf_idx, sqlite3_column_int(stmt2, 0));
                            sqlite3_bind_int(stmt, vec_idx, sqlite3_column_int(stmt3, 0));
                            sqlite3_step(stmt);
                        }
                        sqlite3_reset(stmt2);
                    }
                } else {
                    i = 0;
                    while (i < svec_len) {
                        for ( ; sqlite3_step(stmt3) == SQLITE_ROW && i < svec_len; i++) {
                            sqlite3_step(stmt2);

                            sqlite3_reset(stmt);
                            sqlite3_bind_int(stmt, 1, i);
                            sqlite3_bind_int(stmt, sdf_idx, sqlite3_column_int(stmt2, 0));
                            sqlite3_bind_int(stmt, vec_idx, sqlite3_column_int(stmt3, 0));
                            sqlite3_step(stmt);
                        }
                        sqlite3_reset(stmt3);
                    }
                }
                _sqlite_commit;
            } else { /* not an integer op %% or %/% */
                sprintf(g_sql_buf[2], insert_fmt_string2s, iname, funcname);
                res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
                _sqlite_error(res);

                if (svec_len == op2_len) {
                    for (i = 0; i < svec_len; i++) {
                        sqlite3_step(stmt2); sqlite3_step(stmt3);

                        sqlite3_reset(stmt);
                        sqlite3_bind_int(stmt, 1, i);
                        sqlite3_bind_double(stmt, sdf_idx, sqlite3_column_double(stmt2, 0));
                        sqlite3_bind_double(stmt, vec_idx, sqlite3_column_double(stmt3, 0));
                        sqlite3_step(stmt);
                    }
                } else if (svec_len < op2_len) { /* recycle svec */
                    i = 0;
                    while (i < op2_len) {
                        for ( ; sqlite3_step(stmt2) == SQLITE_ROW && i < op2_len; i++) {
                            sqlite3_step(stmt3);

                            sqlite3_reset(stmt);
                            sqlite3_bind_int(stmt, 1, i);
                            sqlite3_bind_double(stmt, sdf_idx, sqlite3_column_double(stmt2, 0));
                            sqlite3_bind_double(stmt, vec_idx, sqlite3_column_double(stmt3, 0));
                            sqlite3_step(stmt);
                        }
                        sqlite3_reset(stmt2);
                    }
                } else { /* svec_len > op2_len, recycle op2 */
                    i = 0;
                    while (i < svec_len) {
                        for ( ; sqlite3_step(stmt3) == SQLITE_ROW && i < svec_len; i++) {
                            sqlite3_step(stmt2);

                            sqlite3_reset(stmt);
                            sqlite3_bind_int(stmt, 1, i);
                            sqlite3_bind_double(stmt, sdf_idx, sqlite3_column_double(stmt2, 0));
                            sqlite3_bind_double(stmt, vec_idx, sqlite3_column_double(stmt3, 0));
                            sqlite3_step(stmt);
                        }
                        sqlite3_reset(stmt3);
                    }
                }
            }

            sqlite3_finalize(stmt);
            sqlite3_finalize(stmt2);
            sqlite3_finalize(stmt3);
            _sqlite_commit;

            UNUSE_SDF2(iname_op2);
        }
    } else if (functype == 1 && funcname[0] != '!') { /* unary not operator */
        iname = _create_svector1(R_NilValue, "bit", NULL, TRUE);
        sprintf(g_sql_buf[2], "insert into [%s].sdf_data "
                    "select [row name], [%s] == 0 from [%s].sdf_data", 
                    iname, varname_src, iname_src);
        res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
        _sqlite_error(res);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    if (iname != NULL) {
        return _create_svector_sexp(iname, "sdf_data", "V1", 
                (functype == 0) ? "numeric" : "logical");
        UNUSE_SDF2(iname);
    }

    UNUSE_SDF2(iname_src);

    return R_NilValue;

}

SEXP sdf_do_variable_summary(SEXP func, SEXP vector, SEXP na_rm) {
    const char *iname_src, *varname_src, *funcname;
    int res;
    sqlite3_stmt *stmt;
    double _ret = NA_REAL, _ret2; SEXP ret;

    /* get data from arguments (function name and sqlite.vector stuffs) */
    funcname = CHAR_ELT(func, 0);
    iname_src = SDF_INAME(vector);
    varname_src = SVEC_VARNAME(vector);

    if (!USE_SDF1(iname_src, TRUE, FALSE)) return R_NilValue;

    g_narm = LOGICAL(na_rm)[0];
    if (strcmp(funcname, "range") == 0) {
        /* special handling for range. use min then max */
        _init_sqlite_function_accumulator();
        sprintf(g_sql_buf[0], "select min_df([%s]) from [%s].sdf_data", varname_src, iname_src);
        res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, NULL);
        if (_sqlite_error(res)) return R_NilValue;
        sqlite3_step(stmt); 
        _ret = sqlite3_column_double(stmt, 0);
        sqlite3_finalize(stmt);

        if (R_IsNA(_ret) && !g_narm) {
            PROTECT(ret = NEW_NUMERIC(2));
            REAL(ret)[0] = REAL(ret)[1] = R_NaReal;
            goto __sdf_do_variable_summary_out;
        }
        
        _init_sqlite_function_accumulator();
        sprintf(g_sql_buf[0], "select max_df([%s]) from [%s].sdf_data", varname_src, iname_src);
        res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, NULL);
        if (_sqlite_error(res)) return R_NilValue;
        sqlite3_step(stmt); 
        _ret2 = sqlite3_column_double(stmt, 0);
        sqlite3_finalize(stmt);

        /* if there is NA, then the if above should have caught it already */
        PROTECT(ret = NEW_NUMERIC(2));
        REAL(ret)[0] = _ret;
        REAL(ret)[1] = _ret2;
    } else {
        _init_sqlite_function_accumulator();
        sprintf(g_sql_buf[0], "select %s_df([%s]) from [%s].sdf_data", funcname, varname_src, iname_src);
        res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, NULL);
        if (_sqlite_error(res)) return R_NilValue;
        res = sqlite3_step(stmt); 
        _ret = sqlite3_column_double(stmt, 0);
        sqlite3_finalize(stmt);

        if (strcmp(funcname, "all") == 0 || strcmp(funcname, "any") == 0) {
            PROTECT(ret = NEW_LOGICAL(1));
            if (R_IsNA(_ret)) LOGICAL(ret)[0] = NA_INTEGER;
            else LOGICAL(ret)[0] = (_ret != 0);
        } else {
            PROTECT(ret = NEW_NUMERIC(1));
            REAL(ret)[0] = _ret;
        }
    }

__sdf_do_variable_summary_out:
    UNPROTECT(1);
    return ret;
}


SEXP sdf_sort_variable(SEXP svec, SEXP decreasing) {
    const char *iname, *tblname, *varname, *type, *sort_type;
    sqlite3_stmt *stmt;
    int res;

    iname = SDF_INAME(svec);
    tblname = SVEC_TBLNAME(svec);
    varname = SVEC_VARNAME(svec);

    if (!USE_SDF1(iname, TRUE, TRUE)) return R_NilValue;

    /* determine type of svec */
    sprintf(g_sql_buf[0], "select [%s] from [%s].[%s] limit 1", varname, iname, tblname);
    res = sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt, 0);
    _sqlite_error(res);
    sqlite3_step(stmt);
    strcpy(g_sql_buf[0], sqlite3_column_decltype(stmt, 0));
    sqlite3_finalize(stmt);

    if (TEST_SDFVECTORTYPE(svec, "factor")) { /* copy factor table to iname */
        if (TEST_SDFVECTORTYPE(svec, "ordered")) type = "ordered";
        else type = "factor";
        _copy_factor_levels2(type, iname, varname, iname, "V1");
    } else type = CHAR_ELT(GET_SDFVECTORTYPE(svec), 0);

    if (strcmp(tblname, "sdf_data") == 0) {
        /* cache sorted data if column to be sorted comes from main sdf_data */

        /* test if there is already a sort_varname table */
        sort_type = (LOGICAL(decreasing)[0]) ? "desc" : "asc";
        sprintf(g_sql_buf[1], "select count(*) from [%s].sqlite_master "
                "where type='table' and name='sort_%s_%s'", iname, sort_type, varname);
        sqlite3_prepare(g_workspace, g_sql_buf[1], -1, &stmt, NULL);
        sqlite3_step(stmt);
        res = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);

        if (res == 0) {
            sprintf(g_sql_buf[1], "create table [%s].[sort_%s_%s] ([%s] %s)", 
                    iname, sort_type, varname, varname, g_sql_buf[0]);
            if (_sqlite_error(_sqlite_exec(g_sql_buf[1]))) 
                    error("Can't create table: %s", g_sql_buf[1]);

            /* insert to new sdf ordered */
            sprintf(g_sql_buf[0], "insert into [%s].[sort_%s_%s] "
                    "select [%s] from [%s].[%s] order by [%s] %s", 
                    iname, sort_type, varname, varname, iname, tblname, varname, sort_type);
                    
            res = _sqlite_exec(g_sql_buf[0]);
            if (_sqlite_error(res)) error("Can't insert: %s", g_sql_buf[0]);
        }

        UNUSE_SDF2(iname);
        sprintf(g_sql_buf[0], "sort_%s_%s", sort_type, varname);
        return _create_svector_sexp(iname, g_sql_buf[0], varname, type);
    } else {
        /* create a new sdf table, copy data to that */

        /* create a sorted column */

        error("Not yet supported.");
    }

    return R_NilValue;
}

/****************************************************************************
 * VECTOR MATH/OPS/GROUP OPERATIONS
 ****************************************************************************/
struct accumulator_t {
    /* long double acts weird here, but if it's not long double,
     * it f*cks up sum() */
    long double accumulator;
    int started;
};

R_INLINE int __vecmath_checkarg(sqlite3_context *ctx, sqlite3_value *arg, double *value) {
    int ret = 1;
    if (sqlite3_value_type(arg) == SQLITE_NULL) { 
        sqlite3_result_null(ctx); 
        ret = 0;
    } else {
        if (sqlite3_value_type(arg) == SQLITE_INTEGER) 
            *value = sqlite3_value_int(arg); 
        else *value = sqlite3_value_double(arg); 
    }
    return ret;
}

#define SQLITE_MATH_FUNC1(name, func) static void __vecmath_ ## name(\
        sqlite3_context *ctx, int argc, sqlite3_value **argv) { \
    double value; \
    if (__vecmath_checkarg(ctx, argv[0], &value)) { \
        sqlite3_result_double(ctx, func(value)); \
    }  \
}

#define SQLITE_MATH_FUNC2(name, func) static void __vecmath_ ## name(\
        sqlite3_context *ctx, int argc, sqlite3_value **argv) { \
    double value1, value2; \
    if (__vecmath_checkarg(ctx, argv[0], &value1) && \
        __vecmath_checkarg(ctx, argv[1], &value2)) { \
        sqlite3_result_double(ctx, func((long double)value1, (long double)value2)); \
    }  \
}

#define SQLITE_MATH_FUNC_CUM(name, func) static void __vecmath_ ## name(\
        sqlite3_context *ctx, int argc, sqlite3_value **argv) { \
    double value; \
    if (__vecmath_checkarg(ctx, argv[0], &value)) { \
        if (g_start) { g_start = 0; g_accumulator = value; } \
        else g_accumulator = func(g_accumulator, value); \
        sqlite3_result_double(ctx, g_accumulator); \
    }  \
}

#define LOGBASE(a, b) log(a)/log(b)

/* SQLITE_MATH_FUNC1(abs, abs)   in SQLite */
SQLITE_MATH_FUNC1(sign, sign)   /* in R */
SQLITE_MATH_FUNC1(sqrt, sqrt)
SQLITE_MATH_FUNC1(floor, floor)
SQLITE_MATH_FUNC1(ceiling, ceil)
SQLITE_MATH_FUNC1(trunc, ftrunc) /* in R */
/*SQLITE_MATH_FUNC2(round, fprec)  2 arg, in SQLite, but override with R's version */
SQLITE_MATH_FUNC2(signif, fround) /* 2 arg, in R */
SQLITE_MATH_FUNC1(exp, exp)
SQLITE_MATH_FUNC2(log, LOGBASE) /* 2 arg */
SQLITE_MATH_FUNC1(cos, cos)
SQLITE_MATH_FUNC1(sin, sin)
SQLITE_MATH_FUNC1(tan, tan)
SQLITE_MATH_FUNC1(acos, acos)
SQLITE_MATH_FUNC1(asin, asin)
SQLITE_MATH_FUNC1(atan, atan)
SQLITE_MATH_FUNC1(cosh, cosh)
SQLITE_MATH_FUNC1(sinh, sinh)
SQLITE_MATH_FUNC1(tanh, tanh)
SQLITE_MATH_FUNC1(acosh, acosh)  /* nowhere in include?? */
SQLITE_MATH_FUNC1(asinh, asinh)  /* nowhere in include?? */
SQLITE_MATH_FUNC1(atanh, atanh)  /* nowhere in include?? */
SQLITE_MATH_FUNC1(lgamma, lgammafn) /* in R */
SQLITE_MATH_FUNC1(gamma, gammafn) /* in R */
/* SQLITE_MATH_FUNC1(gammaCody, gammaCody)   * in R ?? */
SQLITE_MATH_FUNC1(digamma, digamma) /* in R */    
SQLITE_MATH_FUNC1(trigamma, trigamma) /* in R */

#define SUM(a, b)  ((long double)(a) + (b))
#define PROD(a, b) (a) * (b)
#define MIN(a, b) ((a) <= (b)) ? (a) : (b)
#define MAX(a, b) ((a) >= (b)) ? (a) : (b)
#define ALL(a, b) (((a) == 0) || ((b) == 0)) ? 0 : 1
#define ANY(a, b) (((a) == 0) && ((b) == 0)) ? 0 : 1

SQLITE_MATH_FUNC_CUM(cumsum, SUM)
SQLITE_MATH_FUNC_CUM(cumprod, PROD)
SQLITE_MATH_FUNC_CUM(cummin, MIN)
SQLITE_MATH_FUNC_CUM(cummax, MAX)

#define SQLITE_SUMMARY_FUNC(name, func) static void __vecsummary_ ## name(\
        sqlite3_context *ctx, int argc, sqlite3_value **argv) { \
    double value; \
    struct accumulator_t *acc; \
    acc = sqlite3_aggregate_context(ctx, sizeof(struct accumulator_t)); \
    if (!g_narm && R_IsNA(acc->accumulator)) return; /* NA if na.rm=F & NA found */ \
    if (sqlite3_value_type(argv[0]) != SQLITE_NULL) {  \
        if (sqlite3_value_type(argv[0]) == SQLITE_INTEGER) {  \
            int tmp = sqlite3_value_int(argv[0]); \
            value = (tmp == NA_INTEGER) ? R_NaReal : tmp; \
        } else value = sqlite3_value_double(argv[0]);  \
        if (R_IsNA(value)) { \
            if (!g_narm) acc->accumulator = value; return; /* else ignore */ \
        } else if (!acc->started) { \
            acc->started = 1; \
            acc->accumulator = value; \
        } else acc->accumulator = func(acc->accumulator, value); \
    } \
}

SQLITE_SUMMARY_FUNC(all_df, ALL)
SQLITE_SUMMARY_FUNC(any_df, ANY)
SQLITE_SUMMARY_FUNC(sum_df, SUM)
SQLITE_SUMMARY_FUNC(prod_df, PROD)
SQLITE_SUMMARY_FUNC(min_df, MIN)
SQLITE_SUMMARY_FUNC(max_df, MAX)

static void __vecsummary_finalize(sqlite3_context *ctx) {
    /* g_accumulator already summarizes it. just return that */
    struct accumulator_t *acc = (struct accumulator_t *)
                sqlite3_aggregate_context(ctx, sizeof(struct accumulator_t)); 
    sqlite3_result_double(ctx, acc->accumulator);
}

static void __r_modulo(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL ||
        sqlite3_value_type(argv[1]) == SQLITE_NULL) { 
        sqlite3_result_null(ctx); 
    } else {
        double v1, v2, q, tmp;
        if (sqlite3_value_type(argv[0]) == SQLITE_INTEGER && 
            sqlite3_value_type(argv[1]) == SQLITE_INTEGER) {
            int i1, i2;
            i1 = sqlite3_value_int(argv[0]);
            i2 = sqlite3_value_int(argv[1]);
            if (i1 > 0 && i2 > 0) { 
                sqlite3_result_int(ctx, i1 % i2); return;
            }
            v1 = i1; v2 = i2;
        } else {
            v1 = sqlite3_value_double(argv[0]);
            v2 = sqlite3_value_double(argv[1]);
        }
        /* copied from myfmod() in src/main/arithmetic.c */
        q = v1 / v2; 
        if (v2 == 0) sqlite3_result_double(ctx, R_NaN);
        tmp = v1 - floor(q) * v2;
        /* checking omitted */
        q = floor(tmp/v2);
        sqlite3_result_double(ctx, tmp - q*v2);

    }
}

static void __r_intdiv(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL ||
        sqlite3_value_type(argv[1]) == SQLITE_NULL) { 
        sqlite3_result_null(ctx); 
    } else {
        double v1, v2;
        if (sqlite3_value_type(argv[0]) == SQLITE_INTEGER && 
            sqlite3_value_type(argv[1]) == SQLITE_INTEGER) {
            int i1, i2;
            i1 = sqlite3_value_int(argv[0]);
            i2 = sqlite3_value_int(argv[1]);
            if (i1 == NA_INTEGER || i2 == NA_INTEGER) {
                sqlite3_result_int(ctx, NA_INTEGER); return;
            } else if (i2 == 0) {
                sqlite3_result_int(ctx, 0); return;
            }
            v1 = i1; v2 = i2;
        } else {
            v1 = sqlite3_value_double(argv[0]);
            v2 = sqlite3_value_double(argv[1]);
        }
        /* copied from IDIVOP cases in src/main/arithmetic.c */
        sqlite3_result_double(ctx, floor(v1/v2));
    }
}
#define VMENTRY1(func)  {#func, __vecmath_ ## func}
#define VSENTRY1(func)  {#func, __vecsummary_ ## func}
void __register_vector_math() {
    int i, res;
    static const struct {
        char *name;
        void (*func)(sqlite3_context*, int, sqlite3_value**);
    } arr_func1[] = {
        VMENTRY1(sign),
        VMENTRY1(sqrt),
        VMENTRY1(floor),
        VMENTRY1(ceiling),
        VMENTRY1(trunc),
        VMENTRY1(exp),
        VMENTRY1(cos),
        VMENTRY1(sin),
        VMENTRY1(tan),
        VMENTRY1(acos),
        VMENTRY1(asin),
        VMENTRY1(atan),
        VMENTRY1(cosh),
        VMENTRY1(sinh),
        VMENTRY1(tanh),
        VMENTRY1(acosh),
        VMENTRY1(asinh),
        VMENTRY1(atanh),
        VMENTRY1(lgamma),
        VMENTRY1(gamma),
        VMENTRY1(digamma),
        VMENTRY1(trigamma),
        VMENTRY1(cumsum),
        VMENTRY1(cumprod),
        VMENTRY1(cummin),
        VMENTRY1(cummax)
    }, arr_func2[] =  {
        {"r_mod", __r_modulo},
        {"r_intdiv", __r_intdiv},
        /*VMENTRY1(round),*/
        VMENTRY1(signif),
        VMENTRY1(log)
    }, arr_sum1[] = {
        VSENTRY1(all_df),  /* can't override sum, min, max */
        VSENTRY1(any_df),
        VSENTRY1(sum_df),
        VSENTRY1(prod_df),
        VSENTRY1(min_df),
        VSENTRY1(max_df)
    };

    int len = sizeof(arr_func1) / sizeof(arr_func1[0]);

    for (i = 0; i < len; i++) {
        res = sqlite3_create_function(g_workspace, arr_func1[i].name, 1, 
                SQLITE_ANY, NULL, arr_func1[i].func, NULL, NULL);
        _sqlite_error(res);
    }

    len = sizeof(arr_func2) / sizeof(arr_func2[0]);
    for (i = 0; i < len; i++) {
        res = sqlite3_create_function(g_workspace, arr_func2[i].name, 2, 
                SQLITE_ANY, NULL, arr_func2[i].func, NULL, NULL);
        _sqlite_error(res);
    }

    len = sizeof(arr_sum1) / sizeof(arr_sum1[0]);
    for (i = 0; i < len; i++) {
        res = sqlite3_create_function(g_workspace, arr_sum1[i].name, 1, 
                SQLITE_ANY, NULL, NULL, arr_sum1[i].func, __vecsummary_finalize);
        _sqlite_error(res);
    }
}
