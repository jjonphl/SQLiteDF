library(datasets)

# test if SQLiteDF successfully loaded. it should create (if it does not exist)
# or open workspace.db in the current directory (assuming workspace.db is 
# a valid workspace file)
library(SQLiteDF)
stopifnot(file.exists(".SQLiteDF/workspace.db"))

# test all modes
iris <- cbind(iris, integers=1:10, logicals=c(T,F), chars=c("foo","bar"))

# test creating unnamed sdfs
u1.sdf <- sqlite.data.frame(iris)
stopifnot(file.exists(".SQLiteDF/data1.db"))
u2.sdf <- sqlite.data.frame(attenu)
stopifnot(file.exists(".SQLiteDF/data2.db"))

# test classes
stopifnot(class(u1.sdf) == "sqlite.data.frame",
          class(u2.sdf) == "sqlite.data.frame")

compareSdfToDf <- function(sdf, df, with.names=TRUE) {
    ncols <- ncol(df)
    nrows <- nrow(df)

    # test [.sqlite.data.frame
    for (i in 1:nrows) { for (j in 1:ncols) {
        if (df[i,j] != sdf[i,j]) stop("Not equal on ", i, ",", j, "\n")
    }}

    # test [.sqlite.data.frame for row.names()
    names <- row.names(df)
    for (i in 1:nrows) {
        if (!isTRUE(all.equal(df[names[i]], sdf[names[i]])))
            stop(paste("Not equal on ", names[i], " ([.sqlite.data.frame for row names)"))
    }

    # test [[.sqlite.data.frame (int arg), [.sqlite.vector, 
    # length.sqlite.vector, has.typeSvec
    for (j in 1:ncols) { 
        sv <- sdf[[j]]; # test sqlite.vector
        stopifnot(class(sv) == "sqlite.vector", has.typeSvec(sv, class(df[[j]])[1]))
        if (length(sv) != nrows) stop("Unexpected # of rows for col", j, "\n")
        for (i in 1:nrows)
            if (sv[i] != df[i,j])
                stop("Not equal on row ", i, ", col ", j, "\n")
        all.equal(sv[c(T,F,F)], df[c(T,F,F),j])
    }

    # test names.sqlite.data.frame, [[.sqlite.data.frame (name arg), ...
    if (with.names) for (j in names(df)) { 
        sv <- sdf[[j]]; # test sqlite.vector
        stopifnot(class(sv) == "sqlite.vector", has.typeSvec(sv, class(df[[j]])[1]))
        if (length(sv) != nrows) stop("Unexpected # of rows for col", j, "\n")
        for (i in 1:nrows)
            if (sv[i] != df[i,j]) 
                stop("Not equal on", i, "on col", j, "\n")
    }

    # quote the 1st element of a vector suitable as arg to sdfSelect(where=)
    makewhere <- function(vect, name) {
        paste("[", name, "]=", 
              switch(class(vect),
                numeric=as.character(vect[1]),
                factor=as.character(as.integer(vect[1])),
                ordered=as.character(as.integer(vect[1])),
                character=paste("'", vect[1], "'", sep=""),
                integer=as.character(vect[1]), 
                logical=as.character(as.integer(vect[1]))),
              sep="")
    }

    # test sdfSelect returning vectors
    for (i in 1:ncols) {
        colname = paste("[",names(df)[i],"]", sep="")
        vect <- df[[i]]

        # test where arg with the 1st value of the vector
        testwhere = makewhere(vect, names(df)[i])
        print(testwhere)
            
        stopifnot(all.equal(sdfSelect(sdf, select=colname), vect),
                  all.equal(sdfSelect(sdf, select=colname, limit=nrows-1),
                            vect[1:(nrows-1)]),
                  all.equal(sdfSelect(sdf, select=colname, where=testwhere, debug=T),
                            vect[vect == vect[1]]))
    }

    # test sdfSelect returning data frames
    select = paste(paste("[", names(df), "]", sep=""), collapse=",")
    where = makewhere(df[[1]], names(df)[1])

    # row.names() not equal, & besides sdf returns char & 
    # R has integer row.names
    stopifnot(all.equal(sdfSelect(sdf, select, limit="9,5"), df[10:14,],
                        check.attributes=FALSE),  
              all.equal(sdfSelect(sdf, limit=nrows-1), df[-nrows,],
                        check.attributes=FALSE),
              all.equal(sdfSelect(sdf, where=where), df[df[[1]]==df[1,1],],
                        check.attributes=FALSE))

    
}

# test creating named sdfs
iris.sdf <- sqlite.data.frame(iris, "iris")
stopifnot(file.exists(".SQLiteDF/iris.db"))
iris2.sdf <- sqlite.data.frame(iris, "iris")
stopifnot(file.exists(".SQLiteDF/iris1.db"))

# test lsSdf(), w/c queries workspace.db
lsSdf() 
stopifnot((sort(lsSdf()) == c("data1", "data2", "iris", "iris1")))

# test attach/detach sdf
detachSdf("data1")
stopifnot(sort(lsSdf()) == c("data2", "iris", "iris1"))
attachSdf(".SQLiteDF/data1.db")
stopifnot(sort(lsSdf()) == sort(c("data2", "iris", "iris1", "data1")))

# test row.names, head, tail, "==.character"
stopifnot(all(row.names(u1.sdf)==row.names(iris)),
          all(row.names(u2.sdf)==row.names(attenu)),
          all(head(u1.sdf) == head(iris)),
          all(head(u2.sdf) == head(attenu)),
          all(tail(u1.sdf) == tail(iris)),
          all(tail(u2.sdf) == tail(attenu)))

# test sdf methods
stopifnot(length(iris.sdf) == length(iris),
          length(row.names(iris.sdf)) == length(row.names(iris)),
          all(dim(iris.sdf) == dim(iris)),
          all(names(iris.sdf) == names(iris)))

# test sdf indexers
compareSdfToDf(iris.sdf, iris)
#compareSdfToDf(u2.sdf, attenu) # problem with factors
stopifnot(all.equal(unlist(iris.sdf),unlist(iris.sdf[])),
          all.equal(unlist(iris.sdf),unlist(iris.sdf[,])))
tmp <- iris.sdf[1]     # sdf w/ ncol=1
stopifnot(nrow(tmp) == nrow(iris.sdf),
          names(tmp) == names(iris.sdf)[1],
          all.equal(tmp[,1], iris.sdf[,1]))


# test operators
stopifnot(all.equal(iris.sdf[,1] + iris.sdf[,2], iris[,1] + iris[,2]),
          all.equal(iris.sdf[,1]*10 %/% iris.sdf[,2],iris[,1]*10 %/% iris[,2]),
          all.equal(signif(log(iris.sdf[,1],17)), signif(log(iris[,1],17))),
          all.equal(round(log(iris.sdf[,1]),5), round(log(iris[,1]),5)),
          all.equal(with(iris.sdf, Sepal.Length*Sepal.Width - Petal.Length/Petal.Width), 
                    with(iris, Sepal.Length*Sepal.Width - Petal.Length/Petal.Width)),
          all.equal(sort(iris.sdf[,2]), sort(iris[,2])),
          all.equal(quantile(iris.sdf[,3]), quantile(iris[,3])),
          all.equal(sapply(iris.sdf[,1:4],mean), sapply(iris[,1:4],mean)))

stopifnot(sapply(iris.sdf[,1:4],sum) == sapply(iris[,1:4],sum))


# test [<-.sqlite.vector
ref <- data.frame(
        integer=1:10, real=as.real(1:10), 
        logical=rep(c(T,F,T),length.out=10), 
        character=I(c("one", "two", "three", "four", "five", 
                       "six", "seven", "eight", "nine", "ten")))
ref.sdf <- sqlite.data.frame(ref)
.assign <- function (svec, idx, values, eqvalues=values) {
    idxlen <- if (is.logical(idx)) sum(rep(idx, length.out=length(svec))) else length(idx) 
    if (is.logical(idx)) idx <- rep(idx, length.out=length(svec))
    if (length(values) != idxlen) 
        values <- rep(values, length.out=idxlen)
    if (length(eqvalues) != idxlen)
        eqvalues <- rep(eqvalues, length.out=idxlen)
    svec[idx] <- values 
    all.equal(svec[idx], eqvalues)
}

## test real <- real, int, logical
col = ref.sdf[["real"]]
stopifnot(.assign(col, 1, 25),
          .assign(col, c(1,3,5,7,9), 100),
          .assign(col, c(1,3,5,7,9), c(25, 100)),
          .assign(col, c(2,4,6,8,10), c(T,F,T), as.real(c(T,F,T))), # w/ logicals
          .assign(col, c(4,5,6,7,8), 1:5, as.real(1:5)),            # w/ ints
          .assign(col, c(T,F), 1:5, as.real(1:5)))

# test int <- int, logical
col = ref.sdf[["integer"]]
stopifnot(.assign(col, 1, as.integer(25)),
          .assign(col, c(1,3,5,7,9), as.integer(100)),
          .assign(col, c(1,3,5,7,9), as.integer(c(25, 100))),
          .assign(col, c(2,4,6,8,10), c(T,F,T), as.integer(c(1,0,1))), # w/ logicals
          .assign(col, c(4,5,6,7,8), 1:5),                             # w/ ints
          .assign(col, c(T,F), 1:5))

# test logical <- logical
col = ref.sdf[["logical"]]
stopifnot(.assign(col, 1, F),
          .assign(col, c(1,3,5,7,9), F),
          .assign(col, c(F,T), T))

# test character <- character, real, int, logical
col = ref.sdf[["character"]]
stopifnot(.assign(col, 1, "isa"),
          .assign(col, c(1,3,5,7,9), c("uno", "tatlo", "lima", "pito", "syam")),
          .assign(col, 1, 25, "25"),
          .assign(col, 1, 3/4, "0.75"),                  # w/ reals
          .assign(col, 1:5, as.integer(3), "3"),         # w/ integers
          .assign(col, 4:8, c(T,F), c("TRUE","FALSE")))  # w/ logicals
# test sdfImportDBI
#if (require(RSQLite)) {
#    dr <- SQLite()
#    con <- dbConnect(dr, dbname="example.db")
#    i1 <- sdfImportDBI(con, "select * from iris")
#    compareSdfToDf(i1[,1:4], iris[,1:4], with.names=FALSE)
#    stopifnot(all(as.character(i1[,5]) == as.character(iris[,5])))
#
#    i2 <- sdfImportDBI(con, "select * from iris", 30)  # test rbindSdf
#    compareSdfToDf(i2[,1:4], iris[,1:4], with.names=FALSE)
#    stopifnot(all(as.character(i2[,5]) == as.character(iris[,5])))
#}

# test rbindSdf
rbindSdf(u2.sdf, attenu)
stopifnot(2*nrow(attenu)==nrow(u2.sdf))
#print((nrow(attenu)+1):(nrow(u2.sdf)))
#compareSdfToDf(u2.sdf[(nrow(attenu)+1):(nrow(u2.sdf)),], attenu)

# test sdfImportSQLite
k <- sdfImportSQLite("example.db", "iris")
k1 <- k[,2:6]
for (i in 1:4) stopifnot(all.equal(k1[,i],iris[,i]))
stopifnot(all.equal(levels(k1[,5]),levels(iris[,5])),
          all.equal(as.integer(k1[,5]), as.integer(iris[,5])))

# test summary
for (j in 1:5) stopifnot(all(summary(iris.sdf[,j]) == summary(iris[,j])))
iris.sdf.summary <- summary(iris.sdf)
iris.summary <- summary(iris)
stopifnot(all(iris.sdf.summary[,1:4] == iris.summary[,1:4]))
stopifnot(all(na.exclude(iris.sdf.summary[,5])==na.exclude(iris.summary[,5])))

# test matrices on sdf's
iris.mat <- as.matrix(iris)
iris.smat <- sqlite.matrix(iris)
stopifnot(typeSvec(iris.smat) == mode(iris.mat),
          length(iris.smat) == length(iris.mat),
          all(colnames(iris.smat) == colnames(iris.mat)))
          #all(rownames(iris.smat) == rownames(iris.mat))) # still bug here

