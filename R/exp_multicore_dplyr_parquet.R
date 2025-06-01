# Multicore dplyr on graph .parquet
# Stephen Howe
# 4 March 2025
# Version 1

# versions ----
# v1 20250304 - initial script

# packages ----
library(aws.s3)
library(arrow)
library(duckdb)
library(dplyr)
library(multidplyr)

# data ----
# arrow dataset
da <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/nodes/distinctauthor")

# duckdb table
con <- dbConnect(duckdb::duckdb())
duckdb_register_arrow(con, "distinctAuthor", da)

# speed tests from laptop off network ----
# dplyr
t <- Sys.time()
da |> count() |> collect() # 1017240
Sys.time() - t
# Time difference of 1.503265 mins

# duckdb
t <- Sys.time()
dbGetQuery(con,
           "SELECT count(*) from distinctAuthor") # 1017240
Sys.time() - t
# Time difference of 55.2082 secs

# multidplyr - 10 cores
cluster <- new_cluster(10)
da_part <- da |> partition(cluster) # takes a while ...
t <- Sys.time()
da_part |> count() |> collect() # 1017240
Sys.time() - t

cluster_stop(cluster)
