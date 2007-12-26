#include "sqlite_dataframe.h"
#include "exports.h"
#include <R_ext/Rdynload.h>

static const R_CallMethodDef CallEntries[] = {
    /* sqlite_workspace.c */
    {"sdf_init_workspace", (DL_FUNC) &sdf_init_workspace, 0},
    {"sdf_finalize_workspace", (DL_FUNC) &sdf_finalize_workspace, 0},
    {"sdf_list_sdfs", (DL_FUNC) &sdf_list_sdfs, 1},
    {"sdf_get_sdf", (DL_FUNC) &sdf_get_sdf, 1},
    {"sdf_attach_sdf", (DL_FUNC) &sdf_attach_sdf, 2},
    {"sdf_detach_sdf", (DL_FUNC) &sdf_detach_sdf, 1},
    {"sdf_rename_sdf", (DL_FUNC) &sdf_rename_sdf, 2},

    /* sqlite_dataframe.c */
    {"sdf_create_sdf", (DL_FUNC) &sdf_create_sdf, 2},
    {"sdf_get_names", (DL_FUNC) &sdf_get_names, 1},
    {"sdf_get_length", (DL_FUNC) &sdf_get_length, 1},
    {"sdf_get_row_count", (DL_FUNC) &sdf_get_row_count, 1},
    {"sdf_import_table", (DL_FUNC) &sdf_import_table, 6},
    {"sdf_get_index", (DL_FUNC) &sdf_get_index, 4},
    {"sdf_rbind", (DL_FUNC) &sdf_rbind, 2},
    {"sdf_get_iname", (DL_FUNC) &sdf_get_iname, 1},

    /* sqlite_vector.c */
    {"sdf_get_variable", (DL_FUNC) &sdf_get_variable, 2},
    {"sdf_get_variable_length", (DL_FUNC) &sdf_get_variable_length, 1},
    {"sdf_get_variable_index", (DL_FUNC) &sdf_get_variable_index, 2},
    /* sdf_set_variable_index */
    {"sdf_variable_summary", (DL_FUNC) &sdf_variable_summary, 2},
    {"sdf_do_variable_math", (DL_FUNC) &sdf_do_variable_math, 3},
    {"sdf_do_variable_op", (DL_FUNC) &sdf_do_variable_op, 4},
    {"sdf_do_variable_summary", (DL_FUNC) &sdf_do_variable_summary, 3},
    {"sdf_sort_variable", (DL_FUNC) &sdf_sort_variable, 2},

    /* sqlite_external.c */
    {"sdf_import_sqlite_table", (DL_FUNC) &sdf_import_sqlite_table, 3},

    /* sqlite_matrix.c */
    {"sdf_as_matrix", (DL_FUNC) &sdf_as_matrix, 2},
    {"sdf_create_smat", (DL_FUNC) &sdf_create_smat, 2},

    /* sqlite_biglm.c */
    {"sdf_do_biglm", (DL_FUNC) &sdf_do_biglm, 4},
    {NULL, NULL, 0}
};

void R_init_SQLiteDF(DllInfo *dll) {
    /* Rprintf("I am executed.\n"); */
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
}
