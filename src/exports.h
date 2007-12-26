#ifndef __SQLITE_DATAFRAME__EXPORTS 
#define __SQLITE_DATAFRAME__EXPORTS
/* top level functions */
/* sqlite_workspace.c */
SEXP sdf_init_workspace();
SEXP sdf_finalize_workspace();
SEXP sdf_list_sdfs(SEXP pattern);
SEXP sdf_get_sdf(SEXP name);
SEXP sdf_attach_sdf(SEXP filename, SEXP internal_name);
SEXP sdf_detach_sdf(SEXP internal_name);
SEXP sdf_rename_sdf(SEXP sdf, SEXP name);

/* sqlite_dataframe.c */
SEXP sdf_create_sdf(SEXP df, SEXP name);
SEXP sdf_get_names(SEXP sdf);
SEXP sdf_get_length(SEXP sdf);
SEXP sdf_get_row_count(SEXP sdf);
SEXP sdf_import_table(SEXP _filename, SEXP _name, SEXP _sep, SEXP _quote, 
        SEXP _rownames, SEXP _colnames);
SEXP sdf_get_index(SEXP sdf, SEXP row, SEXP col, SEXP new_sdf);
SEXP sdf_rbind(SEXP sdf, SEXP data);
SEXP sdf_get_iname(SEXP sdf);

/* sqlite_vector.c */
SEXP sdf_get_variable(SEXP sdf, SEXP name);
SEXP sdf_get_variable_length(SEXP svec);
SEXP sdf_get_variable_index(SEXP svec, SEXP idx);
/* SEXP sdf_set_variable_index(SEXP svec, SEXP idx, SEXP value); */
SEXP sdf_variable_summary(SEXP svec, SEXP maxsum);
SEXP sdf_do_variable_math(SEXP func, SEXP vector, SEXP other_args);
SEXP sdf_do_variable_op(SEXP func, SEXP vector, SEXP op2, SEXP arg_reversed);
SEXP sdf_do_variable_summary(SEXP func, SEXP vector, SEXP na_rm);
SEXP sdf_sort_variable(SEXP svec, SEXP decreasing);

/* sqlite_external.c */
SEXP sdf_import_sqlite_table(SEXP _dbfilename, SEXP _tblname, SEXP _sdfiname);

/* sqlite_matrix.c */
SEXP sdf_as_matrix(SEXP sdf, SEXP name);
SEXP sdf_create_smat(SEXP svec, SEXP dimnames);

/* sqlite_biglm.c */
SEXP sdf_do_biglm(SEXP sdfx, SEXP svecy, SEXP sdfx_dim, SEXP intercept);

#endif
