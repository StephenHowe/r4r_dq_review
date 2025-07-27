library(aws.s3)
library(arrow)
library(dplyr)

contributed_to <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/edges/CONTRIBUTED_TO/")

contributed_to |>
  head() |>
  collect() |>
  View()

r4r:00018dd7-909b-435f-bade-6be4b8b8bbd9_rinne_mikael

contribution <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/r4r/phyeng/graph/edges/CONTRIBUTION/")

contribution |>
  head() |>
  collect() |>
  View()

r4r:01JMF01XENQWK8XRCF5F2TY91J
r4r:01JMF01YY67T3VPJQ9CH09B8FY


doi:10.1088/1742-6596/2276/1/012021


opencite <- df |>
  group_by(doi) |>
  summarise(count = length(unique(author_id_true)))
fivenum(opencite$count)

eng_phys <- df |>
  group_by(doi) |>
  summarise(count = length(unique(author_id_pred)))
fivenum(eng_phys$count)


foo_rr <- data.frame(
  doi = c("doi_1", "doi_1", "doi_1", "doi_1", "doi_1"),
  da = c("a1", "a2", "a3", "a4", "a5")
)

foo_oc <- data.frame(
  doi = "doi_1",
  da = "oc_1"
)

foo_df <- foo_rr |>
  left_join(foo_oc, by = "doi")


aws.s3::put_object(
  "aap_acp_k_b3/doi_author_pairs.parquet",
  object = "ext_data/doi_author_pairs.parquet",
  bucket = "com-copyright-prodana-shnpc",
  multipart = TRUE
)


r4rid_dob <- dbGetQuery(
  con,
  "SELECT id, degree_of_belief
  FROM distinctAuthor"
)


# Five num of author count per article
contr |> head(1) |> collect()

article_to_author_count <- dbGetQuery(
  con,
  "SELECT target, count(source)
  FROM contribution
  GROUP BY target"
)


fivenum(article_to_author_count$`count(source)`)
mean(article_to_author_count$`count(source)`)
quantile(article_to_author_count$`count(source)`, 0.9)

# Five num of author count per article for Medline
contr_medl <- open_dataset("s3://com-copyright-agpipeline-sandds/data/internal/parquet/pubmed/medline2016/graph/edges/CONTRIBUTION/")

foo3 <- contr_medl |> head(1) |> collect()
contr_medl |> count() |> collect()

duckdb_register_arrow(con, "contribution_medline", contr_medl)

article_to_author_count_medline <- dbGetQuery(
  con,
  "SELECT target, count(source)
  FROM contribution_medline
  GROUP BY target"
)

fivenum(article_to_author_count_medline$`count(source)`)
mean(article_to_author_count_medline$`count(source)`)
quantile(article_to_author_count_medline$`count(source)`, 0.9)


# Max numbers

dbGetQuery(
  con,
  "SELECT persistent_identifier, recent_articles
  FROM author_docs
  WHERE persistent_identifier = 'r4r:01JV0FDDFX1P8QD2H0NCPH1KT9'"
)

dbGetQuery(
  con,
  "SELECT persistent_identifier, COUNT(recent_articles) AS count_articles
FROM author_docs
GROUP BY persistent_identifier
ORDER BY count_articles DESC
LIMIT 1;"
)


aff |> group_by(source) |> summarise(count = n()) |> arrange(desc(count)) |> head() |> collect() # 43 max
contr |> group_by(source) |> summarise(count = n()) |> arrange(desc(count)) |> head() |> collect() # 307 max
collab |> group_by(source) |> summarise(count = n()) |> arrange(desc(count)) |> head() |> collect() # 10484 !!!

da |> head() |> collect()
fugazi_5 <- da |> select(id, name_variants) |> collect()
fugazi_5 |> unnest() |> group_by(id) |> summarise(count = n()) |> arrange(desc(count)) |> head() # 10

fugazi_6 <- da |> select(id, orcid) |> collect()
fugazi_6 |> unnest() |> group_by(id) |> summarise(count = n()) |> arrange(desc(count)) |> head() # 52


fugazi_7 <- da |> filter(id == "r4r:01JMF01YF6BV00WTBQ7BXKDDDS") |> collect()



