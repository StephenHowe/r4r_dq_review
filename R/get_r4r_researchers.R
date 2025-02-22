# Get R4R Researcher Data for Review
# Stephen Howe
# 22 February 2025
# Version 2

# versions ----
# v1 20250221 - initial script
# v2 20250222 - added complete download of researcher docs in parquet

# packages ----
library(aws.s3)
library(arrow)
library(duckdb)
library(dplyr)

# register datasets ----
da <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/nodes/distinctauthor")
aff <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/edges/AFFILIATION/")
da_proj <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/nodes/r4r_projection_authors/")
con <- dbConnect(duckdb::duckdb())
duckdb_register_arrow(con, "distinctAuthor", da)

# functions ----
getResearcher <- function(x){
  temp <- da_proj |>
    filter(persistent_identifier == x) |>
    collect()
  as.data.frame(temp)
}

# get slices ----
top_10 <- da |>
  select(id, occurrences) |>
  arrange(desc(occurrences)) |>
  head(10) |>
  collect()

top_10_collab <- da |>
  select(id, collaborators_count) |>
  arrange(desc(collaborators_count)) |>
  head(10) |>
  collect()

top_10_rand <- dbGetQuery(
  con,
  "SELECT id FROM distinctAuthor
  ORDER BY RANDOM()
  LIMIT 10"
)

top_10_affiliations <- aff |>
  group_by(source) |>
  summarise(count = n()) |>
  arrange(desc(count)) |>
  head(10) |>
  collect()

top_10_60to70 <- dbGetQuery(
  con,
  "SELECT id, degree_of_belief
  FROM distinctAuthor
  WHERE degree_of_belief >= 0.6
  AND degree_of_belief < 0.7
  ORDER BY RANDOM()
  LIMIT 10"
)

top_10_70to80 <- dbGetQuery(
  con,
  "SELECT id, degree_of_belief
  FROM distinctAuthor
  WHERE degree_of_belief >= 0.7
  AND degree_of_belief < 0.8
  ORDER BY RANDOM()
  LIMIT 10"
)

top_10_80to90 <- dbGetQuery(
  con,
  "SELECT id, degree_of_belief
  FROM distinctAuthor
  WHERE degree_of_belief >= 0.8
  AND degree_of_belief < 0.9
  ORDER BY RANDOM()
  LIMIT 10"
)

top_10_90to95 <- dbGetQuery(
  con,
  "SELECT id, degree_of_belief
  FROM distinctAuthor
  WHERE degree_of_belief >= 0.9
  AND degree_of_belief < 0.95
  ORDER BY RANDOM()
  LIMIT 10"
)

top_10_95to100 <- dbGetQuery(
  con,
  "SELECT id, degree_of_belief
  FROM distinctAuthor
  WHERE degree_of_belief >= 0.95
  AND degree_of_belief < 1
  ORDER BY RANDOM()
  LIMIT 10"
)

# pull docs ----


researcher_docs_top10 <- lapply(top_10$id, getResearcher)
# TODO pull remaining nine doc sets

# format researcher data
getResearcherCore <- function(x, y){
  data.frame(
    category = y,
    r4r_id = x$persistent_identifier,
    family_name = x$family_name,
    given_name = x$given_name,
    email = x$email,
    degree_of_belief = x$degree_of_belief,
    authorship_period_start = as.Date(x$authorship_period[[1]][[1]]),
    authorship_period_end = as.Date(x$authorship_period[[1]][[2]]),
    occurrences = x$occurrences
  )
}

# TODO write out remaining functions

researchers_core <- lapply(data_temp, getResearcherCore, y= "Top 10 Prolific")
researchers_core <- as.data.frame(do.call(rbind, researchers_core))

# TODO run for each set of 10



# combine in single datafame for each and save















































# ### SCRATCH ####
# researcher_core <- data.frame(
#   r4r_id = foo$persistent_identifier,
#   family_name = foo$family_name,
#   given_name = foo$given_name,
#   email = foo$email,
#   degree_of_belief = foo$degree_of_belief,
#   authorship_period_start = as.Date(foo$authorship_period[[1]][[1]]),
#   authorship_period_end = as.Date(foo$authorship_period[[1]][[2]]),
#   occurrences = foo$occurrences
# )
#
# researcher_identifier <- data.frame(
#   r4r_id = foo$persistent_identifier,
#   identifiers = foo$identifiers[[1]]
# )
#
#
#
# researchers <- as.data.frame(do.call(rbind, data_temp_2))
# researchers$category <- "Top 10 by Articles"
#
#
#
#
# # save for app
# saveRDS(
#   researchers,
#   "app/r4r_researcher_review/researchers.rds"
#
# foo1_test <- getResearcher("r4r:01JMF0251C5SSK0K5DXP9W2HGG")
# foo2 <- unlist(foo1_test$identifiers)
#
# duckdb_register_arrow(con, "da_proj_2", da_proj)
#
# # tet query
# foo_test <- dbGetQuery(
#   con,
#   "SELECT * FROM da_proj_2 WHERE persistent_identifier = 'r4r:01JMF0251C5SSK0K5DXP9W2HGG'"
# )
#
#
#
# # downlaod projection ----
# objects <- aws.s3::get_bucket_df(
#   bucket = "com-copyright-agpipeline-sandds",
#   prefix = "data/internal/parquet/r4r/phyeng/graph/nodes/r4r_projection_authors/"
# )
#
# getFiles <- function(x,y){
#   save_object(
#     object = x,
#     bucket = "com-copyright-agpipeline-sandds",
#     file = paste0("data/r4r_", y, ".parquet")
#   )
# }
#
# # test function
# # getFiles(
# #   "data/internal/parquet/r4r/phyeng/graph/nodes/r4r_projection_authors/part-00000-bc00116c-17a7-47e1-846b-1b6e171f616c-c000.snappy.parquet",
# #   "001"
# # )
#
# # run function en masse
# my_list <- objects$Key
# my_numbers <-rep(paste0("proj_", 1:nrow(objects)))
#
# mapply(getFiles, my_list, my_numbers)
#
# # duckdb connection to parquet docs
# con <- dbConnect(duckdb::duckdb())
# dbExecute(con, "CREATE TABLE da_proj AS SELECT * FROM read_parquet('data/*.parquet')")
#
#
# # data slices ----
# top_10 <- dbGetQuery(
#   con,
#   "SELECT *
#   FROM da_proj
#   ORDER BY occurrences DESC
#   LIMIT 10"
# )
#
# top_collab <- dbGetQuery(
#   con,
#   "SELECT *
#   FROM da_proj
#   ORDER BY colla"
# )
#
#
#
#
#
#
#
# my_parquet <- list.files("data/",
#                          full.names = TRUE)
# data_dir <- "/Users/stephenhowe/Documents/code_active/r4r_dq_review/data"
# da_proj <-- open_dataset(
#   data_dir,
#   format = "parquet"
# )
#
# read_parquet(my_parquet[[1]])
# da <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/nodes/distinctauthor")
# da_proj <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/nodes/r4r_projection_authors/")
# # au_docs <- open_dataset("s3://com-copyright-agpipeline-sandds/data/output/phyeng/json/graph/nodes/", format = "json")
#
#
#
#
# # save for app ----
#
# getSaveJson <- function(x){
#   da_proj |>
#     filter(persistent_identifier == x) |>
#     collect()
# }
#
#
#
# # test and scratch
# foo_test <- getSaveJson("r4r:01JMF0251C5SSK0K5DXP9W2HGG")
