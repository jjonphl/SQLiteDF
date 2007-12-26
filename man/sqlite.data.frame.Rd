\name{sqlite.data.frame}
\alias{sqlite.data.frame}
\alias{sdf}
\title{SQLite Data Frame}
\description{ Creates an Sqlite Data Frame (SDF) from ordinary data frames.  }
\usage{
sqlite.data.frame(x, name=NULL)
}
\arguments{
  \item{x}{The object to be coerced into a data frame which is then stored in a 
SQLite database. \code{as.data.frame} is called first on x before creating
the SDF database. }
  \item{name}{The internal name of the SDF. If none is provided, a generic
name \emph{data}<n> is used (e.g. data1, data2, etc). Each SDF should have a unique
internal name and also be a valid R symbol. Numbers are appended to names in case of duplicates, 
e.g. if name arg is \emph{iris}, and it already exists, then the
new SDF will have a name \emph{iris1}. If it still exists, then the name
will be \emph{iris2}, and so on. }
}
\details{
SQLite data frames (SDF's) are data frames whose data are stored in a SQLite database.
SQLite is an open source, powerful (considering its size), light weight data base engine.
It stores each database (composed of tables, indices, etc.) in a single file. Since a
single SDF occupies a whole database, each SDF will be contained in a single file.

Each SDF file contains the following tables:
\itemize{
\item{sdf\_attributes}{a key-value table that contains the SDF attributes. Currently,
only \emph{name} is used representing the SDF's internal name.}
\item{sdf\_data}{contains the actual data. Factors and ordered variables are stored as
integers. Their levels are stored in other tables. Numeric (real) are stored as double,
characters as text and integers as int's. Currently, complex numbers are not supported.
Column names correspond exactly to the variable names of the ordinary data frames. E.g.
Petal.Length will have a column name Petal.Length in the table. This is possible
because SQLite allows almost any kind of column name as long as it is quoted by 
square brakets ([ ]). You're on your own if you try to be a smartass on this. Also,
an extra column named \emph{row name} (with the space between the words), of type
text is used to store the data frame row names and is set as the table's primary key.
So please don't use \emph{row name} as a variable name.}
\item{[factor <colname>] and [ordered <colname>]}{stores the levels and level labels
for each factor variable in the SDF. One such table will be created for every
factor or ordered var, even if two variables share the same level labels. Besides
storing the level data, it is used to mark a column as being a factor.}
}

SDF's are managed in a workspace separate from R's. When SQLiteDF is loaded, it searches
for the file \code{workspace.db} inside the subdirectory \code{.SQLiteDF} in the current 
working directory. This file contains
a list of SDF's created/used in the previous session (i.e. SQLiteDF sessions are automatically
saved), including their full and relative path and attach information. 
Workspace is managed using the SQLite engine
by opening \code{workspace.db} as the main database and then attaching (SQLite's attach) the SDF's.
Unfortunately, the number of attached databases is limited to 31 (actually 32, but 1 is reserved
for the temp db). Therefore, SDF's are \emph{scored} according to the number of times
it has been used. When the maximum allowed attachment is reached, the least used attached
SDF's is detached and the needed one is attached in its place. On compiling the package, the
configure script modifies the bundled SQLite source such that
constant controlling the maximum attachments is modified to 31 (default is 10).

Back to when SQLiteDF is loaded, after opening \code{workspace.db}, the SDF's stored in the list
are sorted according to their number of uses in the previous session and then the 
first 30 are attached. The relative path is used for finding the SQLite file. If the file
cannot be found, it is deleted from the SQLiteDF workspace (with a warning message). The
scores are then all reset.

A sqlite.data.frame object is a list a single element:
\itemize{
\item{iname}{the internal name of the SDF.}
}
and the following attributes:
\itemize{
\item{class}{The S3 class vector \code{c("sqlite.data.frame", "data.frame")}}
\item{row.names}{A sqlite.vector of mode character containing the row names of the SDF}
}

All SDF's created in the session will have their SQLite file stored in the subdirectory .SQLiteDF in the
current working directory. SDF's created in the other session can be imported/attached
to the current SDF workspace using \code{attachSdf}, which may reside anywhere in the
file system. 
}
\value{
A S3 object representing the SDF. The SDF database will be created in the same directory
with file name derived by appending the extension \emph{db} to the passed internal
name, or the default internal name if none is provided.
}
\author{Miguel A. R. Manese}
\note{
The full path is used to avoid attaching the same db which may have different relative path
after the user changes directory after loading SQLiteDF (see \code{attachSdf}).
}
\seealso{
    \code{\link[SQLiteDF]{lsSdf}}
    \code{\link[SQLiteDF]{getSdf}}
    \code{\link[SQLiteDF]{attachSdf}}
    \code{\link[SQLiteDF]{detachSdf}}
}
\examples{
    library(datasets)
    iris.sdf <- sqlite.data.frame(iris)
    names(iris.sdf)
    class(iris.sdf)
    iris.sdf$Petal.Length[1:10]
    iris.sdf[["Petal.Length"]][1:10]
    iris.sdf[1,c(TRUE,FALSE)]
    #apply(iris.sdf[1:4], 2, mean)
}
\keyword{data}
\keyword{manip}
\keyword{classes}
