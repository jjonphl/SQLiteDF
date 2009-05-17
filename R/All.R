.onLoad <- function(libname, pkgname) {
    tryCatch({setwd(".SQLiteDF"); setwd("..")}, error=function (e) dir.create(".SQLiteDF"))
    .Call("sdf_init_workspace")
}

.onUnload <- function(libpath) {
    .Call("sdf_finalize_workspace")
    library.dynam.unload("SQLiteDF", libpath)
}

sdf_tempdir <- function() .Call("sdf_tempdir")
# -------------------------------------------------------------------------
# workspace functions
# -------------------------------------------------------------------------
lsSdf <- function(pattern=NULL) .Call("sdf_list_sdfs", pattern)
getSdf <- function(name) .Call("sdf_get_sdf", name)

readTableSdf <- function(filename, sep=",", quote="\"'", name=NULL, 
                            rownames, colnames)
    .Call("sdf_import_table", filename, name, sep, quote, rownames, colnames)

attachSdf <- function(sdf_filename, sdf_iname=NULL) 
    invisible(.Call("sdf_attach_sdf", sdf_filename, sdf_iname))
detachSdf <- function(iname) .Call("sdf_detach_sdf", iname)

# -------------------------------------------------------------------------
# sqlite.vector functions
# -------------------------------------------------------------------------
sqlite.vector <- function(vec, name=NULL) {
    if (!is.atomic(vec)) stop("vec is not an atomic vector")
    tmp <- data.frame(V1=vec)
    ret <- sqlite.data.frame(tmp, name)
    ret$V1
}
typeSvec <- function(x) attr(x, "sdf.vector.type")
has.typeSvec <- function(x, type) {
    if (inherits(x, "sqlite.vector")) typeSvec(x) == type else FALSE
}

# -------------------------------------------------------------------------
# sqlite.data.frame functions
# -------------------------------------------------------------------------
"sqlite.data.frame" <- function(x, name=NULL) {
    if (inherits(x, "sqlite.data.frame")) x 
    else .Call("sdf_create_sdf", as.data.frame(x), name)
}

dupSdf <- function(sdf) { 
    if (!inherits(sdf, "sqlite.data.frame")) stop("Not a sqlite.data.frame.")
    sdf[1:length(sdf)]
}
renameSdf <- function(sdf, name) { 
    if (!inherits(sdf, "sqlite.data.frame")) stop("Not a sqlite.data.frame.")
    if (!is.character(name)) stop("name argument must be a string.")
    .Call("sdf_rename_sdf", sdf, name)
}
inameSdf <- function(sdf) .Call("sdf_get_iname", sdf)

is.sqlite.data.frame <- function(x) inherits(x, "sqlite.data.frame")

# -------------------------------------------------------------------------
# sqlite.matrix functions
# -------------------------------------------------------------------------
.smat.fix.dimnames <- function(data.dimnames, data.dim=sapply(data.dimnames,length)) {
    if (is.null(data.dimnames)) {
        data.dimnames <- list(as.character(1:data.dim[1]), as.character(1:data.dim[2]))
    } else {
        if (is.null(data.dimnames[[1]])) 
            data.dimnames[[1]] <- as.character(1:data.dim[1])
        if (is.null(data.dimnames[[2]]))
            data.dimnames[[2]] <- as.character(1:data.dim[2])
    }
    data.dimnames
}

sqlite.matrix <- function(data, name=NULL) {
    if (inherits(data, "sqlite.matrix")) data
    else if (inherits(data, "sqlite.data.frame")) .Call("sdf_as_matrix", data, name)
    else {
        data <- as.matrix(data)
        data.dim <- dim(data)
        data.dimnames <- dimnames(data)
        dim(data) <- NULL
        vec <- sqlite.vector(data)
        data.dimnames <- .smat.fix.dimnames(data.dimnames, data.dim)
        if (typeSvec(vec) %in% c("factor", "ordered")) {
            vec <- unclass(vec)
            attr(vec, "levels") <- NULL
            attr(vec, "sdf.vector.type") <- "character"
        }
        return(.Call("sdf_create_smat", vec, data.dimnames))
    }
}

is.sqlite.matrix <- function(x) inherits(x, "sqlite.matrix")

# -------------------------------------------------------------------------
# external data functions
# -------------------------------------------------------------------------
sdfImportDBI <- function(con, sql, batch.size=2048, rownames="row_names", iname = NULL) {
    on.exit(dbClearResult(rs))
    rs <- dbSendQuery(con, sql)
    df <- fetch(rs, batch.size)
    if (length(rownames) > 1) stop("more than one column containing row names?")

    if (is.numeric(rownames)) has_rn <- rownames
    else if (is.character(rownames)) has_rn <- (1:length(df))[names(df) == rownames]

    if (length(has_rn) == 0) has_rn <- FALSE

    if (has_rn) { 
        rn <- df[,has_rn]; df <- df[,-has_rn]; row.names(df) <- rn
    }

    sdf <- sqlite.data.frame(df, iname)
    rowname <- batch.size
    while (! dbHasCompleted(rs)) {
        df <- fetch(rs, batch.size)
        if (has_rn) { 
            rn <- df[,has_rn]; df <- df[,-has_rn]; row.names(df) <- rn
        }
        rbindSdf(sdf, df)
    }
    sdf
}

sdfImportSQLite <- function(dbfilename, tablename, iname = tablename) {
    .Call("sdf_import_sqlite_table", dbfilename, tablename, iname)
}

sdfImportText <- function(file, iname=NULL, sep="", quote="\"'", dec=".", as.is=FALSE, 
        na.strings="NA", colClasses=NA, skip=0, fill=!blank.lines.skip, 
        strip.white=FALSE, blank.lines.skip=FALSE, comment.char="#", allowEscapes=FALSE, 
        flush=FALSE, batch.size=2048) {  
    
    data <- read.table(file=file,sep=sep,quote=quote,dec=dec,as.is=as.is,na.strings=na.strings,
                colClasses=colClasses,skip=skip,fill=fill,strip.white=strip.white,
                blank.lines.skip=blank.lines.skip,comment.char=comment.char,
                allowEscapes=allowEscapes,flush=flush,nrows=batch.size)
    sdf <- sqlite.data.frame(data, iname)
    
    if (length(colClasses) < length(data) || colClasses == NA) 
        colClasses <- sapply(data, function(x) class(x)[1]);

    sskip <- skip;
    while (nrow(data) == batch.size) {
        sskip <- sskip + batch.size
        data <- read.table(file=file,sep=sep,quote=quote,dec=dec,as.is=as.is,
                    na.strings=na.strings, colClasses=colClasses,skip=sskip,fill=fill,
                    strip.white=strip.white, blank.lines.skip=blank.lines.skip,
                    comment.char=comment.char, allowEscapes=allowEscapes,flush=flush,
                    nrows=batch.size)
        rbindSdf(sdf, data)
    }
    sdf
}

sdfSelect <- function(sdf, select=NULL, where=NULL, limit=NULL, debug=FALSE) {
    if (!is.sqlite.data.frame(sdf)) stop("sdf must be an sqlite.data.frame")
    if (!is.null(limit)) limit = as.character(limit)
    .Call("sdf_select", sdf, select, where, limit, debug)
}
    

# -------------------------------------------------------------------------
# overriden primitives
# -------------------------------------------------------------------------
ver = paste(R.version$major, R.version$minor[1], sep=".")
if (ver < "2.4.0") {
    sort.default <- base::sort 
    sort <- function(x, ...) UseMethod("sort")
    formals(sort.default) <- c(formals(sort.default), alist(...=))
    
    # to make use of sort()
    median <- function(x, na.rm=FALSE) as.numeric(quantile(x, 0.5, na.rm=na.rm))

    # to get generic sort
    environment(quantile.default) <- .GlobalEnv
}


# -------------------------------------------------------------------------
# biglm stuffs
# -------------------------------------------------------------------------
sdflm <- function(formula, sdf, batch.size=1024) {
    n <- 1:batch.size
    sdf.nrows <- nrow(sdf)
    res <- biglm:::biglm(formula, sdf[n,])
    n <- n + batch.size
    while (n[1] < sdf.nrows) {
        if (n[batch.size] > sdf.nrows) n <- n[1]:sdf.nrows
        res <- biglm:::update(res, sdf[n,])
        n <- n + batch.size
    }
    res
}

sdflm2 <- function(x, y, intercept=TRUE) {
    if (!inherits(y, "sqlite.vector")) stop("y must be a sqlite.vector")
    if (! attr(y, "sdf.vector.type") %in% c("numeric", "integer"))
        stop("y must be a numeric sqlite.vector")
    if (!inherits(x, "sqlite.data.frame")) stop("x must be a sqlite.data.frame")
    if (! all(sapply(x, function (x) attr(x, "sdf.vector.type")) %in% c("numeric", "integer")))
        stop("all columns of x must be numeric sqlite.vector-s")
    if (nrow(x) != length(y)) stop("rows of x and length of y not equal")

    rval <- .Call("sdf_do_biglm", x, y, dim(x), intercept)
    if (intercept) rval$names <- c("(Intercept)", names(x))
    else rval$names <- names(x)
    rval$n <- nrow(x)
    rval
}

summary.sdflm <- function(object,...){
    beta<-coef(object)
    se<-sqrt(diag(vcov(object)))
    mat<-cbind(`Coef`=beta, `(95%`=beta-2*se, `CI)`=beta+2*se, `SE`=se,
               `p`=2*pnorm(abs(beta/se),lower.tail=FALSE))
    rownames(mat)<-object$names
    rval<-list(obj=object, mat=mat)
    class(rval)<-c("summary.sdflm", "summary.biglm")
    rval
}

# -------------------------------------------------------------------------
# S3 methods for sqlite.data.frame
# -------------------------------------------------------------------------
names.sqlite.data.frame <- function(x) .Call("sdf_get_names", x)
length.sqlite.data.frame <- function(x) .Call("sdf_get_length", x)
nrow.sqlite.data.frame <- function(x) .Call("sdf_get_row_count", x)
dim.sqlite.data.frame <- function(x)
    c(nrow.sqlite.data.frame(x), length.sqlite.data.frame(x))
dimnames.sqlite.data.frame <- function(x) list(row.names(x), names(x))
"$.sqlite.data.frame" <- function(x, name) .Call("sdf_get_variable", x, name)
"[[.sqlite.data.frame" <- function(x, idx) {
    if (length(idx) != 1) stop("index must be a 1-element vector.")
    if (is.character(idx)) .Call("sdf_get_variable", x, idx)
    else if (is.numeric(idx)) {
        if (idx > length(x)) stop("subscript out of bounds")
        else .Call("sdf_get_variable", x, names(x)[idx])
    } else stop("don't know how to handle index.")
}
"[.sqlite.data.frame" <- function(x, row, col) {
    Narg <- nargs()
    if (Narg == 3) {
        if (missing(row) && missing(col)) return(x)  # x[,]
        if (missing(row)) row = NULL
        if (missing(col)) col = NULL
        if (is.null(row) && is.null(col)) return(data.frame())
        return(.Call("sdf_get_index", x, row, col, FALSE))
    } else if (Narg == 2) {
        if (missing(row)) return(x)  # x[]
        if (is.null(row)) return(data.frame())
        return(.Call("sdf_get_index", x, NULL, row, TRUE))
    }
}

"[<-.sqlite.data.frame" <- function(x, i=NA, j=NA, value) {
    # nargs either 3 (df[], df[i]) or 4 (df[,], df[i,], df[,j], df[i,j]
    # value is never missing
}

as.list.sqlite.data.frame <- function(x, ...) {
    ret <- list()
    for (i in names(x)) ret[[i]] <- x[[i]]
    ret
}
is.list.sqlite.data.frame <- function(x) FALSE;
rbindSdf <- function(sdf, df) {
    .Call("sdf_rbind", sdf, df)
}
with.sqlite.data.frame <- function(data, expr, ...)  
    eval(substitute(expr), as.list(data), enclos=parent.frame())
as.data.frame.sqlite.data.frame <- function(x, ...) x
as.matrix.sqlite.data.frame <- function(x, ...) {
    args <- as.list(...)
    if ("name" %in% as.list) name <- args$name else name <- NULL
    sqlite.matrix(x, name)
}

row.names.sqlite.data.frame <- function(x) attr(x, "sdf.row.names")

# row.names are overwritten with 1:n
head.sqlite.data.frame <- function(x, n = 6, ...) {
    xrows <- nrow(x)
    if (n > xrows) {
        n <- min(6, xrows)
        warning(paste("Number of rows specified exceeds the SDF's number of rows.",
            "Trimming down to ", n, " rows", sep=""))
    }
    x[1:n,]
}
tail.sqlite.data.frame <- function(x, n = 6, ...) {
    rows <- nrow(x); x[(rows-n+1):rows,]
}

    
print.sqlite.data.frame <- function(x, n = 6, ...) {
    xdim <- dim(x)
    xnames <- inameSdf(x)
    n <- min(xdim[1], n)
    cat(paste("SQLite data frame \"", xnames[1], "\" (",
              xdim[1], " row(s) by ", xdim[2],
              " column(s)) stored in file \"",
              xnames[2], "\"\n\n", sep = ""))
    cat(paste("First", n, "rows:\n"))
    print(head(x, n, ...))
    if (xdim[1] > n) cat(" ...\n")
}
      
summary.sqlite.data.frame <- function(object, maxsum=7, digits=max(3, getOption("digits")-3), ...)
    base:::summary.data.frame(object, maxsum, digits, ...)

# -------------------------------------------------------------------------
# S3 methods for sqlite.vector
# -------------------------------------------------------------------------
"[.sqlite.vector" <- function(x, idx) {
    # temporary, better to be in C because assumption is length(x) is large
    if (is.numeric(idx) && all(idx <= 0)) idx <- (1:length(x))[idx]
    .Call("sdf_get_variable_index", x, idx)
}

"[<-.sqlite.vector" <- function(x, idx, value) {
    .Call("sdf_set_variable_index", x, idx, value)
}
length.sqlite.vector <- function(x) .Call("sdf_get_variable_length", x)
is.list.sqlite.vector <- function(x) FALSE
# methods to "coerce" to ordinary vectors
as.double.sqlite.vector <- function(x, ...) as.double(x[1:length(x)])
as.character.sqlite.vector <- function(x, ...) as.character(x[1:length(x)])
as.logical.sqlite.vector <- function(x, ...) as.logical(x[1:length(x)])
as.integer.sqlite.vector <- function(x, ...) as.integer(x[1:length(x)])
Math.sqlite.vector <- function(x, ...) {
    if (any(has.typeSvec(x, "factor"), has.typeSvec(x, "ordered")))
        stop(paste(.Generic, "not meaningful for factors"))
    if (!any(has.typeSvec(x, "numeric"), has.typeSvec(x, "integer")))
        stop("Non-numeric argument to mathematical function")
    #.Generic
    other.args <- formals(args(get(.Generic, mode="function")))[-1]
    extra.args <- list(...)

    # "union" of list elements, with values in extra.args taking precedence
    # to get default values if missing.
    # there is some "magic" with Math group functions: they already perform
    # checking on number of args, ... is passed without the original param names,
    # and even if you do round(digits=3,5.23512) ... will be list(3)
    if (length(extra.args) > 0) other.args[1:length(extra.args)] <- extra.args

    # as of 2.4.0, the most # of args in any of the func under Math is 2.
    # the 2nd arg is tricky, since it can be a vector > 1 then we'd have
    # to take care of recycling etc. simplify by allowing only scalars
    # as 2nd arg.
    if (length(other.args) > 0) {
        argnames <- names(other.args)
        
        if (is.call(other.args[[argnames[1]]])) 
            other.args[[argnames[1]]] <- eval(other.args[[argnames[1]]])
        if (length(other.args[[argnames[1]]]) > 1) 
            stop(paste("non scalar", argnames[1], "is not supported"))
        if (is.null(other.args[[argnames[1]]]))
            stop(paste("NULL", argnames[1], "is not supported"))
    }

    ret <- .Call("sdf_do_variable_math", .Generic, x, other.args)
    if (is.character(ret)) { file.remove(ret); ret <- NULL }
    ret;
}

Summary.sqlite.vector <- function(x, ..., na.rm=F) {
    if (!any(has.typeSvec(x, "numeric"), has.typeSvec(x, "integer"), has.typeSvec(x, "logical")))
        stop("Non-numeric argument")
    ret <- .Call("sdf_do_variable_summary", .Generic, x, as.logical(na.rm))
    if (is.character(ret)) { file.remove(ret); ret <- NULL }
    ret
}
Ops.sqlite.vector <- function(e1, e2) {
    if (any(has.typeSvec(e1, "factor"), has.typeSvec(e2, "factor"),
            inherits(e1, "factor"), inherits(e2, "factor")))
        stop("not meaningful for factors")
    arg.reversed <- FALSE
    if (!inherits(e1, "sqlite.vector")) { 
        tmp <- e1; e1 <- e2; e2 <- tmp; arg.reversed = TRUE; 
    }
    # if e2 is not sqlite.vector nor atomic vector, come what may
    .Call("sdf_do_variable_op", .Generic, e1, e2, arg.reversed)
}

sort.sqlite.vector <- function(x, decreasing=FALSE, ...) {
    .Call("sdf_sort_variable", x, as.logical(decreasing))
}

#quantile.sqlite.vector <- function(x, probs=seq(0,1,0.25), names=FALSE,
#        na.rm=FALSE, type=7, ...) NextMethod()

summary.sqlite.vector <- function(object, maxsum=100, digits=max(3, getOption("digits")-3), ...) {
    if (has.typeSvec(object, "factor") || has.typeSvec(object, "ordered") || has.typeSvec(object, "logical")) 
        .Call("sdf_variable_summary", object, as.integer(maxsum))
    else if (has.typeSvec(object, "numeric") || has.typeSvec(object, "integer")) {
        # copied from summary.default
        qq <- quantile(object)
        qq <- signif(c(qq[1:3], mean(object), qq[4:5]), digits)
        names(qq) <- c("Min.", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max.")
        class(qq) <- "table"
        qq
    } else if (has.typeSvec(object, "character")) {
        ret <- c(as.character(length(object)), "sqlite.vector", "character")
        names(ret) <- c("Length", "Class", "Type")
        class(ret) <- "table"
        ret
    } else stop(paste("not implemented for type ", typeSvec(object), sep=""))
}

mean.sqlite.vector <- function(x, ...) {
    if (!(has.typeSvec(x, "numeric") || has.typeSvec(x, "integer") || has.typeSvec(x, "logical"))) {
        warning("argument is not numeric or logical: returning NA")
        return(as.numeric(NA))
    }
    sum(x) / length(x)
}

is.sqlite.vector <- function(x) inherits(x, "sqlite.vector")

all.equal.sqlite.vector <- function(target, current, batch.size=1024, ...) {
    len <- length(target)
    if (len != length(current)) 
        return(paste("sqlite.vector: lengths(", len, ", ", length(current),
                        ") differ", sep=""))
    batch.all.equal <- function(x, y) {
        i <- 1;
        while (i < len) {
            last <- min(i+batch.size, len)
            if (!isTRUE(all.equal(target[i:last], current[i:last], ...)))
                return(FALSE)
            i <- i + batch.size
        }
        return(TRUE)
    }
    if (is.sqlite.vector(target) & is.sqlite.vector(current)) {
        if (typeSvec(target) != typeSvec(current))
            return(paste("target is ", typeSvec(target), " sqlite.vector, current is ",
                    typeSvec(current), " sqlite.vector", sep=""))
        # not the most efficient, but the quickest to code
        return(batch.all.equal(target, current))
    } else if (typeSvec(target) == class(current)[1]) {
        return(batch.all.equal(target, current))
    }
    return(FALSE)
}

print.sqlite.vector <- function(x, n = 6, ...) {
    xdim <- length(x)
    xlist <- as.list(x)
    xnames <- inameSdf(x)
    n <- min(xdim, n)
    cat(paste("SQLite vector (",
              xdim, " elements)",
              " of type ", typeSvec(x), "\n",
              "Column \"", xlist$varname, "\" in SQLite data frame \"",
              xnames[1], "\" stored in file \"",
              xnames[2], "\"\n\n", sep = ""))
    cat(paste("First", n, "elements:\n"))
    print(x[1:n], ...)
    if (xdim > n) cat(" ...\n")
}
# -------------------------------------------------------------------------
# S3 methods for sqlite.matrix
# -------------------------------------------------------------------------
length.sqlite.matrix <- function(x) .Call("sdf_get_variable_length", x)
dim.sqlite.matrix <- function(x) attr(x, "sdf.dim") # nrow(), ncol()
dimnames.sqlite.matrix <- function(x) attr(x, "sdf.dimnames") # rownames(), colnames()
head.sqlite.matrix <- function(x, n=6, ...) {
    mdim <- dim(x)
    mrows <- mdim[1]
    if (n > mrows) {
        n <- min(6, mrows)
        warning(paste("Number of rows specified exceeds the SDF's number of rows.",
            "Trimming down to ", n, " rows", sep=""))
    }
    start.idx <- seq(1, mdim[1]*mdim[2], by=mdim[1])
    stopifnot(length(start.idx) == mdim[2])
    idx <- sapply(start.idx, function(x) x:(x+n-1))
    ret <- .Call("sdf_get_variable_index", x, idx)
    if (is.null(ret)) return(invisible(NULL))
    ret <- matrix(ret, n, mdim[2])
    colnames(ret) <- colnames(x)
    rownames(ret) <- rownames(x)[1:n]
    ret
}
print.sqlite.matrix <- function(x, n = 6, ...) {
    xdim <- dim(x)
    xnames <- inameSdf(x)
    cat(paste("SQLite matrix \"", xnames[1], "\" (",
              xdim[1], " rows by ", xdim[2],
              " column(s)) stored in file \"",
              xnames[2], "\"\n\n", sep = ""))
    cat(paste("First", n, "rows:\n"))
    print(head(x, n, ...))
    if (xdim[1] > n) cat(" ...\n")
}
"[.sqlite.matrix" <- function(x, row, col) {
    Narg <- nargs()
    Ncol <- ncol(x)
    Nrow <- nrow(x)
    return.matrix <- function(x, row, col) {
        ncolx <- length(col); nrowx <- length(row)
        idxcol <- Nrow * (col - 1)   # base-0 index of 1st column elems
        idx <- as.numeric(sapply(idxcol, function(x) x + row))
        ret <- .Call("sdf_get_variable_index", x, idx)
        if (ncolx > 1 && nrowx > 1) {
            ret <- matrix(x[idx], nrow=length(row), ncolx)
            colnames(ret) <- colnames(x)[col]
            rownames(ret) <- rownames(x)[row]
        } else if (ncolx > 1) names(ret) <- colnames(x)[col]
        else if (nrowx > 1) names(ret) <- row
         
        return(ret)
    }

    if (Narg == 3) {
        if (missing(row) && missing(col)) return(x)   # x[,]
        if (missing(row)) return(.Call("sdf_get_matrix_columns", x, col))  # x[,m]
        if (missing(col)) return(return.matrix(x, row, 1:Ncol))  # x[n,]
        if (is.null(row) && is.null(col)) return (matrix(nrow=0,ncol=0))
        return(return.matrix(x, row, col)) # x[n,m]
    } else if (Narg == 2) {
        if (missing(row)) return(x)  # x[]
        if (is.null(row)) return(numeric(0)) # x[NULL]
        return(.Call("sdf_get_variable_index", x, row)) # x[n]
    }
}

Ops.sqlite.matrix <- function(e1, e2) {
    arg.reversed <- FALSE
    if (!inherits(e1, "sqlite.matrix")) { 
        tmp <- e1; e1 <- e2; e2 <- tmp; arg.reversed = TRUE; 
    }
    if (!all(dim(e1) == dim(e2))) stop("non-conformable arrays")
    ret <- Ops.sqlite.vector(e1, e2)
    if (arg.reversed) {
        if (is.atomic(e2)) {
            e2.dimnames <- .smat.fix.dimnames(dimnames(e2), dim(e2))
            return(.Call("sdf_create_smat", ret, e2.dimnames))
        } else if (is.sqlite.matrix(e2)) {
            return(.Call("sdf_create_smat", ret, dimnames(e2)))
        }
    } else return(.Call("sdf_create_smat", ret, dimnames(e1)))
}
    

# -------------------------------------------------------------------------
# S3 methods for sdflm
# -------------------------------------------------------------------------
#coef.sdflm <- function(object, ...) {
#    rval <- biglm:::coef.biglm(object, ...)
#    names(rval) <- names(rval$X)
#    rval
#}

update.sdflm <- function(object, moredata, ...) {
    stop("not updatable")
}
