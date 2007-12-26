#include "sqlite_dataframe.h"
#include <math.h>

#define TOL 1e-12
#define ZERO 0.000000000000000000
#define EQUAL_ZERO(x) (abs((x)) < TOL)

void F77_NAME(includ)(int *, int *, double *, double *, double *,
		      double *, double *, double *, double *, int *);

/* EQUAL_ZERO gives error */
static void __include(int np, int nrbar, double *D, double *rbar, double *thetab, 
        double *sserr, double *xrow, double y, double weight) {
    int nextr = 0, i, k;
    long double xi, xk, di, wxi, dpi, cbar, sbar;
    for (i = 0; i < np; i++) {
        if (weight == ZERO) return;
        xi = xrow[i];
        if (xi == ZERO) { nextr += np - i; continue; }
        di = D[i];
        wxi = weight * xi;
        dpi = di + wxi * wxi;
        cbar = di / dpi;
        sbar = wxi / dpi;
        weight *= cbar;
        D[i] = dpi;

        if (i < np - 1) {
            for (k = i+1; k < np; k++) {
                xk = xrow[k];
                xrow[k] = xk - xi * rbar[nextr];
                rbar[nextr] = cbar * rbar[nextr] + sbar * xk;
                nextr++;
            }
            xk = y;
            y = xk - xi * thetab[i];
            thetab[i] = cbar * thetab[i] + sbar * xk;
        }

        *sserr += weight * y * y;
    }
}


SEXP sdf_do_biglm(SEXP sdfx, SEXP svecy, SEXP sdfx_dim, SEXP intercept) {
    SEXP sdflm, qr, D, rbar, thetab, tmp;
    int np, nrbar, nprotect = 0, intrcpt, i, ier;
    double *xrow, y, *ptrD, *ptrrbar, *ptrthetab, sserr, weight;
    sqlite3_stmt *stmt1, *stmt2;
    const char *inamex, *inamey, *tblnamey, *varnamey;

    inamex = SDF_INAME(sdfx);

    inamey = SDF_INAME(svecy);
    tblnamey = SVEC_TBLNAME(svecy);
    varnamey = SVEC_VARNAME(svecy);

    if (!USE_SDF1(inamex, TRUE, TRUE)) return R_NilValue;
    if (!USE_SDF1(inamey, TRUE, TRUE)) return R_NilValue;

    /* get number of columns for sdfx */
    np = INTEGER(sdfx_dim)[1]; 
    intrcpt = (LOGICAL(intercept)[0]) ? 1 : 0;
    np += intrcpt;
    nrbar = (np*(np-1))/2;

    /* Rprintf("np: %d, nrbar %d\n", np, nrbar); */

    PROTECT(D = NEW_NUMERIC(np)); nprotect++;
    PROTECT(rbar = NEW_NUMERIC(nrbar)); nprotect++;
    PROTECT(thetab = NEW_NUMERIC(np)); nprotect++;
    xrow = (double *)R_alloc(np, sizeof(double));

    /* initialize bigqr values */
    if (intrcpt) xrow[0] = 1.0;
    sserr = 0.0;
    ptrD = REAL(D); ptrrbar = REAL(rbar); ptrthetab = REAL(thetab);
    for (i = 0; i < np; i++) ptrD[i] = ptrrbar[i] = ptrthetab[i] = 0.0;
    for ( ; i < nrbar; i++) ptrrbar[i] = 0.0;

    /* setup sqlite_stmt's */
    sprintf(g_sql_buf[0], "select * from [%s].sdf_data", inamex);
    _sqlite_error(sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt1, NULL));
    sprintf(g_sql_buf[0], "select [%s] from [%s].[%s]", varnamey, inamey, tblnamey);
    _sqlite_error(sqlite3_prepare(g_workspace, g_sql_buf[0], -1, &stmt2, NULL));

    if (intrcpt) {
        while (sqlite3_step(stmt1) == SQLITE_ROW) {
            sqlite3_step(stmt2);
            for (i = 1; i < np; i++) 
                xrow[i] = sqlite3_column_double(stmt1, i);
            y = sqlite3_column_double(stmt2, 0);
            weight = 1.0;
            /*__include(np, nrbar, ptrD, ptrrbar, ptrthetab, &sserr, xrow, y, 1.0); */
            F77_CALL(includ)(&np, &nrbar, &weight, xrow, &y, ptrD, ptrrbar, ptrthetab, &sserr, &ier);
        }
    } else { 
        while (sqlite3_step(stmt1) == SQLITE_ROW) {
            sqlite3_step(stmt2);
            for (i = 0; i < np; i++)
                xrow[i] = sqlite3_column_double(stmt1, i+1);
            y = sqlite3_column_double(stmt2, 0);
            /*__include(np, nrbar, ptrD, ptrrbar, ptrthetab, &sserr, xrow, y, 1.0); */
            weight = 1;
            F77_CALL(includ)(&np, &nrbar, &weight, xrow, &y, ptrD, ptrrbar, ptrthetab, &sserr, &ier);
        }
    }

    sqlite3_finalize(stmt1);
    sqlite3_finalize(stmt2);

    /* setup bigqr SEXP */
    PROTECT(qr = NEW_LIST(6)); nprotect++;

    /* set names */
    PROTECT(tmp = NEW_CHARACTER(6)); nprotect++;
    SET_STRING_ELT(tmp, 0, mkChar("D"));
    SET_STRING_ELT(tmp, 1, mkChar("rbar"));
    SET_STRING_ELT(tmp, 2, mkChar("thetab"));
    SET_STRING_ELT(tmp, 3, mkChar("ss"));
    SET_STRING_ELT(tmp, 4, mkChar("checked"));
    SET_STRING_ELT(tmp, 5, mkChar("tol"));
    SET_NAMES(qr, tmp);

    /* set values */
    SET_VECTOR_ELT(qr, 0, D);
    SET_VECTOR_ELT(qr, 1, rbar);
    SET_VECTOR_ELT(qr, 2, thetab);
    SET_VECTOR_ELT(qr, 3, ScalarReal(sserr));
    SET_VECTOR_ELT(qr, 4, ScalarLogical(FALSE));
    PROTECT(tmp = NEW_NUMERIC(np)); nprotect++;
    for (i = 0; i < np; i++) REAL(tmp)[i] = 0.0;
    SET_VECTOR_ELT(qr, 5, tmp);

    /* set class */
    SET_CLASS(qr, mkString("bigqr"));

    /* setup sdflm SEXP */
    PROTECT(sdflm = NEW_LIST(3)); nprotect++;
    
    PROTECT(tmp = NEW_CHARACTER(3)); nprotect++;
    SET_STRING_ELT(tmp, 0, mkChar("qr"));
    SET_STRING_ELT(tmp, 1, mkChar("X"));
    SET_STRING_ELT(tmp, 2, mkChar("intercept"));
    SET_NAMES(sdflm, tmp);

    SET_VECTOR_ELT(sdflm, 0, qr);
    SET_VECTOR_ELT(sdflm, 1, duplicate(sdfx));
    SET_VECTOR_ELT(sdflm, 2, ScalarLogical(intrcpt));

    PROTECT(tmp = NEW_CHARACTER(2)); nprotect++;
    SET_STRING_ELT(tmp, 0, mkChar("sdflm"));
    SET_STRING_ELT(tmp, 1, mkChar("biglm"));
    SET_CLASS(sdflm, tmp);

    UNPROTECT(nprotect);
    UNUSE_SDF2(inamex);
    UNUSE_SDF2(inamey);
    return sdflm;
}
