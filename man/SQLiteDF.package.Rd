\name{SQLiteDF-package}
\alias{SQLiteDF-package}
\alias{SQLiteDF}
\docType{package}
\title{ SQLite data frames }
\description{ S3 and utility methods to implement SQLite data frames. }
\details{
This package aims to transparently operate on data frames stored in
SQLite databases. The following functions are known to be working with
SQLiteDF types:\\

\strong{sqlite.vector}
\itemize{
\item{Math, Summary, Ops groups}{except for those with 2 args in Math
like \code{round}, and \code{\%\%} and \code{\%/\%} in Ops}  
\item{summary}{out of the box works for numeric, integer, character. 
Also works for logical, factor using "group by", however factor's do not
handle NA's specially. }
\item{length}{Ok}
\item{\code{\[}}{not yet \code{\[<-}}
\item{sort}{sorts to a new sqlite.vector}
}

\strong{sqlite.data.frame}
\itemize{
\item{length}{returns no. of variables}
\item{dimnames, names, row.names}{returns names of variables and rows}
\item{dim, nrow, ncol}{dimension of variables}
\item{\code{\$}, \code{[[}}{returns columns as sqlite.vector. no assignments yet}
\item{\code{[}}{works for numeric & integer indexing (ranges and vectors),
logical indexes with recycling. does not support negative and character (row/column name) indexing.}
\item{is.list}{returns false, so that other functions can }
\item{rbind}{works only when rbind()-ing data frames with the same names()}
\item{with}{Ok}
\item{as.data.frame}{just returns itself}
\item{as.list}{returns a list of sqlite.vector for each variable}
\item{rbind}{currently works only with data.frame's}
\item{lapply, sapply}{out of the box}
\item{summary}{out of the box, after implementing sqlite.vector}
}
}
\author{
Miguel A. R. Manese
Maintainer: Miguel A. R. Manese <jjonphl@gmail.com>
}
\keyword{ package }
