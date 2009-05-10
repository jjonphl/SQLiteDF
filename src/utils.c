#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <unistd.h>
#include "R.h"
#include "Rdefines.h"
#include "Rinternals.h"

#define __UTIL__
#include "sqlite_dataframe.h"

int _is_r_sym(const char *sym) {
    int i, len = strlen(sym);

    if (isalpha(sym[0]) || sym[0] == '_') i = 1;
    else if (sym[0] == '.') { if (isdigit(sym[1])) return FALSE; i = 2; }
    else return FALSE;

    for (; i < len; i++) {
        if (!(isalnum(sym[i]) || sym[i] == '.' || sym[i] == '_')) return FALSE;
    }

    return TRUE;
}

int _file_exists(char *filename) {
    FILE *f; int ret = FALSE;
    if ((f = fopen(filename, "rb")) != NULL) { fclose(f); ret = TRUE; }
    return ret;
}

int _empty_callback(void *data, int ncols, char **rows, char **cols) {
    return 0;
}

char *_fixname(char *rname) {
    char *tmp = rname;
    while (*tmp) { if (*tmp == '.') *tmp = '_'; tmp++; }
    return rname;
}


char *_r2iname(char *rname, char *iname) {
    /* basically s/[.!@#$%^&*()-+=]/_/g */
    strcpy(iname, rname);
    char *tmp = iname;
    while ((tmp = strpbrk(tmp, ".!@#$%^&*()-+=")) == NULL) *tmp = '_';
    return iname;
}

int _check_sql_buf(int i) {
    int ret = strlen(g_sql_buf[i]);
    if (i >= NBUFS) return FALSE;
    if ((ret*1.0/g_sql_buf_sz[i]) > 0.6) {
        g_sql_buf_sz[i] *= 2;
        g_sql_buf[i] = Realloc(g_sql_buf[i], g_sql_buf_sz[i], char);
    } else ret = 0;
    return ret;
} 

/*R_INLINE*/ void  _expand_buf(int i, int size) {
    if (size >= g_sql_buf_sz[i]) {
        char *new_buf;
        int new_size = g_sql_buf_sz[i] * 2;
        if (size > new_size) new_size = size;
        Rprintf("Reallocating buffer (%d): %d\n", i, new_size);
        new_buf = Realloc(g_sql_buf[i], new_size, char);

        if (new_buf != NULL) {
            g_sql_buf[i] = new_buf;
            g_sql_buf_sz[i] = new_size;
        }
    }
}

/*R_INLINE*/ int _sqlite_error_check(int res, const char *file, int line) {
    int ret = FALSE;
    if (res != SQLITE_OK) { 
        Rprintf("SQLITE ERROR (line %d at %s): %s\n", line, file, sqlite3_errmsg(g_workspace));
        ret = TRUE;
    }
    return ret;
}

const char *_get_column_type(const char *class, int type) {
    if (type == INTSXP) return "int";
    else if (type == REALSXP) return "double";
    else if (type == STRSXP) return "text";
    else if (type == LGLSXP) return "bit";
    else if (strcmp(class, "factor") == 0) return "int"; /* do I really reach this ? */
    else if (strcmp(class, "ordered") == 0) return "int";
    
    return NULL;
}

int _get_r_type(const char *decl_type) {
    if (strcmp(decl_type, "int") == 0) return INTSXP;
    else if (strcmp(decl_type, "double") == 0) return REALSXP;
    else if (strcmp(decl_type, "text") == 0) return STRSXP;
    else if (strcmp(decl_type, "bit") == 0) return LGLSXP;
    return -1;
}

const char *_get_r_class(const char *db, const char *type) {
    return NULL;
}

int _get_row_count2(const char *table, int quote) {
    int ret, res;
    sqlite3_stmt *stmt;

    if (quote) sprintf(g_sql_buf[2], "select count(*) from [%s].sdf_data", table);
    else sprintf(g_sql_buf[2], "select count(*) from %s", table);
   
    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, NULL);
    if (res != SQLITE_OK) return -1;
    sqlite3_step(stmt);
    ret = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);
    return ret;
}

/* TODO: windows version */
char *_get_full_pathname2(const char *relpath) {
    char *tmp1, *tmp2, tmp3, tmp4;
    int buflen, relpathlen;

    relpathlen = strlen(relpath);
    if (relpath[0] == '/') {
        _expand_buf(2, relpathlen);
        strcpy(g_sql_buf[2], "/");
        buflen = 1;
    } else {
        while (TRUE) {
            tmp1 = getcwd(g_sql_buf[2], g_sql_buf_sz[2]);
            if (tmp1 == NULL) {
                _expand_buf(2, g_sql_buf_sz[2]+1);
            } else { 
                buflen = strlen(tmp1); 
                strcpy(g_sql_buf[2]+buflen,"/");
                buflen += 1;
                break;
            }
        }
    }

    /* we'll go along the relpath string "normalizing" relative paths */
    tmp1 = strcpy(g_sql_buf[3], relpath);
    while (tmp1[0]) {
        tmp2 = tmp1;
        while(!(*tmp2 == '/' || *tmp2 == 0)) tmp2++;
        
        tmp3 = *tmp2;
        *tmp2 = 0; /* temporarily "end" string at that point */
        if (strcmp(tmp1, ".") == 0) {
            /* nothing to do */
        } else if (strcmp(tmp1, "..") == 0) {
            /* remove top dir */
            if (buflen > 1) {
                buflen--;
                do buflen--; 
                while (g_sql_buf[2][buflen] != '/' && buflen > 1);
                g_sql_buf[2][++buflen] = 0;
            }
        } else { 
            /* non-relative path part, append to buf */
            tmp4 = 0;
            if (tmp3 == '/') { *tmp2 = '/'; tmp4 = *(tmp2+1); *(tmp2+1) = 0; }
            _expand_buf(2, buflen+strlen(tmp1));
            strcpy(g_sql_buf[2] + buflen, tmp1);
            buflen += strlen(tmp1);
            if (tmp3 == '/') { *(tmp2+1) = tmp4; }
        }

        if (tmp3 == 0) break;
        else { *tmp2 = '/'; tmp1 = tmp2 + 1; }
    }

    return g_sql_buf[2];
}

/* based on p70 of R-exts.pdf */
SEXP _getListElement(SEXP list, const char *varname) {
    SEXP ret = R_NilValue, names = GET_NAMES(list);
    int i;

    for (i = 0; i < LENGTH(list); i++) {
        if (strcmp(CHAR(STRING_ELT(names, i)), varname) == 0) {
            ret = VECTOR_ELT(list, i);
            break;
        }
    }

    return ret;
}

/* return the row names of an sdf as a sqlite.vector */
SEXP _get_rownames2(const char *sdf_iname) {
    int res;
    sqlite3_stmt *stmt;

    sprintf(g_sql_buf[2], "select [row name] from [%s].sdf_data", sdf_iname);
    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);

    sqlite3_finalize(stmt);
    if (_sqlite_error(res)) return R_NilValue; 

    return _create_svector_sexp(sdf_iname, "sdf_data", "row name", "character");
}

SEXP _create_sdf_sexp(const char *iname) {
    SEXP names, class, variable, ret;
    int nprotected = 0;
    PROTECT(ret = NEW_LIST(1)); nprotected++;
    PROTECT(names = mkString("iname")); nprotected++;
    SET_NAMES(ret, names);

    PROTECT(variable = mkString(iname)); nprotected++;
    SET_VECTOR_ELT(ret, 0, variable);

    /* set class */
    PROTECT(class = NEW_CHARACTER(1)); nprotected++;
    SET_STRING_ELT(class, 0, mkChar("sqlite.data.frame"));
    SET_CLASS(ret, class);
    SET_SDFROWNAMES(ret, _get_rownames2(iname));
    
    UNPROTECT(nprotected);
    return ret;
}

static void __attach_levels2(char *table, SEXP var, int len) {
    /* arg table is assumed to be surrounded by [] already */
    SEXP levels;
    int idx = 0, res;
    sqlite3_stmt *stmt;

    PROTECT(levels = NEW_CHARACTER(len));
    sprintf(g_sql_buf[2], "select level, label from %s order by level asc",
            table);
    res = sqlite3_prepare(g_workspace, g_sql_buf[2], -1, &stmt, 0);
    _sqlite_error(res);
    while (sqlite3_step(stmt) == SQLITE_ROW) { 
        SET_STRING_ELT(levels, idx, mkChar((char *)sqlite3_column_text(stmt, 1)));
        idx++;
    }
    SET_LEVELS(var, levels);
    UNPROTECT(1);
}
    
/* attaches level values and class attributes to the SEXP var if it has class 
 * factor and/or ordered */
int _get_factor_levels1(const char *iname, const char *varname, SEXP var, int set_class) {
    int res;
    SEXP class;
    
    sprintf(g_sql_buf[1], "[%s].[factor %s]", iname, varname);
    res = _get_row_count2(g_sql_buf[1], 0);
    if (res > 0) { /* res is exptected to be {-1} \union I+ */
        __attach_levels2(g_sql_buf[1], var, res);
        if (set_class) {
            PROTECT(class = mkString("factor"));
            SET_CLASS(var, class);
            UNPROTECT(1);
        }
        return VAR_FACTOR;
    }

    sprintf(g_sql_buf[1], "[%s].[ordered %s]", iname, varname);
    res = _get_row_count2(g_sql_buf[1], 0);
    if (res > 0) {
        __attach_levels2(g_sql_buf[1], var, res);
        if (set_class) {
            PROTECT(class = NEW_CHARACTER(2));
            SET_STRING_ELT(class, 0, mkChar("ordered"));
            SET_STRING_ELT(class, 1, mkChar("factor"));
            SET_CLASS(var, class);
            UNPROTECT(1);
        }
        return VAR_ORDERED;
    }

    /*
    if (set_class) {
        PROTECT(class = NEW_CHARACTER(1));
        SET_STRING_ELT(class, 0, mkChar("integer"));
        SET_CLASS(var, class);
        UNPROTECT(1);
    }*/

    return VAR_INTEGER;
}

SEXP _shrink_vector(SEXP vec, int len) {
    int origlen = LENGTH(vec);
    SEXP ret = vec;

    if (vec == R_NilValue) return vec;
    else if (origlen > len) {
        int type = TYPEOF(vec), i;
        if (type == CHARSXP || type == STRSXP) {
            PROTECT(ret = NEW_CHARACTER(len));
            for (i = 0; i < len; i++) {
                SET_STRING_ELT(ret, i, STRING_ELT(vec, i));
            }
        } else if (type == INTSXP) {
            /* for non-strings, memcpy is more efficient but ... */
            PROTECT(ret = NEW_INTEGER(len));
            for (i = 0; i < len; i++) INTEGER(ret)[i] = INTEGER(vec)[i];
        } else if (type == REALSXP) {
            PROTECT(ret = NEW_NUMERIC(len));
            for (i = 0; i < len; i++) REAL(ret)[i] = REAL(vec)[i];
        } else if (type == LGLSXP) {
            PROTECT(ret = NEW_LOGICAL(len));
            for (i = 0; i < len; i++) LOGICAL(ret)[i] = LOGICAL(vec)[i];
        } else return ret;

        /* preserve class, levels for factors/ordered */
        if (isFactor(vec)) {
            SET_CLASS(ret, duplicate(GET_CLASS(vec)));
            SET_LEVELS(ret, duplicate(GET_LEVELS(vec)));
        }

        UNPROTECT(1);
    }

    return ret;
}

/* prepare sdf workspace before attaching a new db */
int _prepare_attach2() {
    sqlite3_stmt *stmt;
    int nloaded;

    sqlite3_prepare(g_workspace, "select count(*) from workspace where loaded=1",
            -1, &stmt, NULL);
    sqlite3_step(stmt);
    nloaded = sqlite3_column_int(stmt, 0);
    sqlite3_finalize(stmt);

    /* test if we have to detach somebody */
    if (nloaded == MAX_ATTACHED) {
        /* have to evict */
        char *iname2;
        sqlite3_prepare(g_workspace, "select internal_name from workspace "
                "where loaded=1 and used=0 order by uses", -1, &stmt, NULL);
        sqlite3_step(stmt);
        iname2 = (char *)sqlite3_column_text(stmt, 0);
        sprintf(g_sql_buf[2], "detach [%s]", iname2);
        _sqlite_error(_sqlite_exec(g_sql_buf[2]));
        sprintf(g_sql_buf[2], "update workspace set loaded=0 where internal_name='%s'", iname2);
        sqlite3_finalize(stmt); 
        _sqlite_error(_sqlite_exec(g_sql_buf[2]));
    }

    return nloaded == MAX_ATTACHED;
}

char *_str_tolower(char *out, const char *ref) {
    int i;
    for (i = 0; ref[i]; i++) out[i] = tolower(ref[i]);
    out[i] = 0;
    return out;
}

/* note that *plen must be initialized with the # of rows of the indexed
 * vector. this is used win idx is a boolean, so that we can recycle it to
 * span the whole vector.
 * NOTE: index returned is 1-based */
int *_make_row_index(SEXP idx, int *plen) {
    int idxlen = LENGTH(idx), len = *plen, i, k;
    int *ret;

    if (IS_NUMERIC(idx)) {
        double tmp;
        ret = (int *)R_alloc(idxlen, sizeof(int));
        for (i = k = 0; i < idxlen; i++) {
            tmp = REAL(idx)[i];
            if (!(tmp == NA_REAL || tmp < 1)) ret[k++] = (int) tmp;

            /*
            if (tmp == NA_REAL) ret[i] = NA_INTEGER;
            else if (tmp < 1) ret[i] = NA_INTEGER;
            else ret[i] = (int) tmp;
            */
        }
        *plen = k;
    } else if (IS_INTEGER(idx)) {
        int tmp;
        ret = (int *)R_alloc(idxlen, sizeof(int));
        for (i = k = 0; i < idxlen; i++) {
            tmp = INTEGER(idx)[i];
            if (!(tmp == NA_REAL || tmp < 1)) ret[k++] = (int) tmp;
            /*
            if (tmp < 1) ret[i] = NA_INTEGER;
            else ret[i] = tmp;
            */
        }
        *plen = k;
    } else if (IS_LOGICAL(idx)) {
        int k, tmp;  /* to deal with recycling */
        ret = (int *)R_alloc(len, sizeof(int));
        for (i = k = 0; i < len; i++) {
            /* i index the values, j the index sexp, k the output index */
            if (LOGICAL(idx)[i%idxlen]) ret[k++] = i+1;
            /* ret[k++] = (LOGICAL(idx)[j%idxlen]) ? i : NA_INTEGER; */
        }
        *plen = k;
    }

    return ret;
}
