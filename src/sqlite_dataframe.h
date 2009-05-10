#include "R.h"
#include "Rdefines.h"
#include "Rinternals.h"
#include "sqlite3.h"
#include "exports.h"

#ifndef __SQLITE_DATAFRAME__
#define __SQLITE_DATAFRAME__

/* # of sql buffers */
#define NBUFS 4

#ifdef __SQLITE_WORKSPACE__
SEXP SDF_RowNamesSymbol;
SEXP SDF_VectorTypeSymbol;
SEXP SDF_DimSymbol;
SEXP SDF_DimNamesSymbol;
#else 
extern SEXP SDF_RowNamesSymbol;
extern SEXP SDF_VectorTypeSymbol;
extern SEXP SDF_DimSymbol;
extern SEXP SDF_DimNamesSymbol;

extern sqlite3 *g_workspace;
extern char *g_sql_buf[NBUFS];
extern int g_sql_buf_sz[NBUFS];
#endif

/* sdf type attributes */
#define GET_SDFVECTORTYPE(x) getAttrib(x, SDF_VectorTypeSymbol) 
#define SET_SDFVECTORTYPE(x,t) setAttrib(x, SDF_VectorTypeSymbol, t) 
#define TEST_SDFVECTORTYPE(x, t) (strcmp(CHAR(asChar(GET_SDFVECTORTYPE(x))), t) == 0)
#define GET_SDFROWNAMES(x) getAttrib(x, SDF_RowNamesSymbol)
#define SET_SDFROWNAMES(x, r) setAttrib(x, SDF_RowNamesSymbol, r)

#ifndef SET_ROWNAMES
#define SET_ROWNAMES(x, r) setAttrib(x, R_RowNamesSymbol, r)
#endif

#define WORKSPACE_COLUMNS 6
#define MAX_ATTACHED 30     /* 31 including workspace.db */

/* utilities for checking characteristics of arg */
int _is_r_sym(const char *sym);
int _file_exists(char *filename);
int _sdf_exists2(const char *iname);

/* sdf utilities */
int USE_SDF1(const char *iname, int exists, int protect);  /* call this before doing anything on an SDF */
int UNUSE_SDF2(const char *iname); /* somewhat like UNPROTECT */
SEXP _create_sdf_sexp(const char *iname);  /* create a SEXP for an SDF */
int _add_sdf1(const char *filename, const char *internal_name); /* add SDF to workspace */
void _delete_sdf2(const char *iname); /* remove SDF from workspace */
int _get_factor_levels1(const char *iname, const char *varname, SEXP var, int set_class);
int _get_row_count2(const char *table, int quote);
SEXP _get_rownames(const char *sdf_iname);
char *_get_full_pathname2(const char *relpath); /* get full path given relpath, used in workspace mgmt */
int _is_factor2(const char *iname, const char *factor_type, const char *colname);
SEXP _get_rownames2(const char *sdf_iname);

/* utilities for creating SDFs */
char *_create_sdf_skeleton1(SEXP name, int *o_namelen, int protect);
int _copy_factor_levels2(const char *factor_type, const char *iname_src,
        const char *colname_src, const char *iname_dst, const char *colname_dst);
int _create_factor_table2(const char *iname, const char *factor_type, 
        const char *colname);
char *_create_svector1(SEXP name, const char *type, int * _namelen, int protect);

/* utilities for creating SVecs */
int _get_vector_index_typed_result(sqlite3_stmt *stmt, SEXP *ret, int colidx, 
        int idx_or_len, int *coltype);

/* R utilities */
SEXP _getListElement(SEXP list, const char *varname);
SEXP _shrink_vector(SEXP vec, int len); /* shrink vector size */

/* sqlite utilities */
int _empty_callback(void *data, int ncols, char **row, char **cols);
int _sqlite_error_check(int res, const char *file, int line);
const char *_get_column_type(const char *class, int type); /* get sqlite type corresponding to R class & type */
int _get_r_type(const char *decl_type);                   /* get r type based on sqlite decltype */
sqlite3* _is_sqlitedb(const char *filename);
void _init_sqlite_function_accumulator();
int *_make_row_index(SEXP idx, int *plen);

/* global buffer (g_sql_buf) utilities */
/*R_INLINE*/ void _expand_buf(int i, int size);  /* expand ith buf if size > buf[i].size


/* workspace utilities */
int _prepare_attach2();  /* prepare workspace before attaching a sqlite db */

/* sqlite vector utilities */
SEXP _create_svector_sexp(const char *iname, const char *tblname, const char *varname, const char *type);

/* misc utilities */
char *_r2iname(char *internal_name, char *filename);
char *_fixname(char *rname);
char *_str_tolower(char *out, const char *ref);

/* register functions to sqlite */
void __register_vector_math();

#define _sqlite_exec(sql) sqlite3_exec(g_workspace, sql, NULL, NULL, NULL)
#define _sqlite_error(res) _sqlite_error_check((res), __FILE__, __LINE__)

#ifdef __SQLITE_DEBUG__
#define _sqlite_begin  { _sqlite_error(_sqlite_exec("begin")); Rprintf("begin at "  __FILE__  " line %d\n",  __LINE__); }
#define _sqlite_commit  { _sqlite_error(_sqlite_exec("commit")); Rprintf("commit at "  __FILE__  " line %d\n",  __LINE__); }
#else
#define _sqlite_begin  _sqlite_error(_sqlite_exec("begin")) 
#define _sqlite_commit _sqlite_error(_sqlite_exec("commit"))
#endif

/* R object accessors shortcuts */
#define CHAR_ELT(str, i) CHAR(STRING_ELT(str,i))

/* SDF object accessors shortcuts */
#define SDF_INAME(sdf) CHAR(STRING_ELT(_getListElement(sdf, "iname"),0))
#define SVEC_TBLNAME(sdf) CHAR(STRING_ELT(_getListElement(sdf, "tblname"),0))
#define SVEC_VARNAME(sdf) CHAR(STRING_ELT(_getListElement(sdf, "varname"),0))

/* formerly R_INLINE-d functions */
/* define _expand_buf(i, size) { \
    if ((size) >= g_sql_buf_sz[i]) { \
        g_sql_buf_sz[i] *= 2; \
        g_sql_buf[i] = Realloc(g_sql_buf[i], g_sql_buf_sz[i], char); \
    } \
} */


/* possible var types when stored in sqlite as integer */
#define VAR_INTEGER 0
#define VAR_FACTOR  1
#define VAR_ORDERED 2

/* detail constants (see _get_sdf_detail2 in sqlite_workspace.c) */
#define SDF_DETAIL_EXISTS 0
#define SDF_DETAIL_FULLFILENAME 1

/* R SXP type constants */
#define FACTORSXP 11
#define ORDEREDSXP 12

/* additional column types */
#define SQLITEDF_BIT 100

#endif
