# Get R4R Researcher Data for Review
# Stephen Howe
# 22 February 2025
# Version 3

# versions ----
# v1 20250221 - initial script
# v2 20250222 - added complete download of researcher docs in parquet
# v3 20250222 - added slices; working with parquet; created multiple dataframes for each slice

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

getResearcherCore <- function(x, y){
  data.frame(
    category = y,
    r4r_id = x$persistent_identifier,
    family_name = x$family_name,
    given_name = x$given_name,
    degree_of_belief = x$degree_of_belief,
    authorship_period_start = ifelse(is.null(x$authorship_period[[1]]), "no date", as.character(x$authorship_period[[1]][[1]])),
    authorship_period_end = ifelse(is.null(x$authorship_period[[1]]), "no date", as.character(x$authorship_period[[1]][[2]])),
    occurrences = x$occurrences
  )
}

getResearcherIdentifiers <- function(x){
  data.frame(
    r4r_id = x$persistent_identifier,
    identifiers = x$identifiers[[1]]
  )
}

getResearcherNameVar <- function(x){
  data.frame(
    r4r_id = x$persistent_identifier,
    alt_name = x$name_variants[[1]]$alternate_name,
    work_count = x$name_variants[[1]]$work_count
  )
}

getResearcherArticles <- function(x){
  if (is.null(x$recent_articles[[1]])) {

    data.frame(
      r4r_id = x$persistent_identifier,
      name = "no article",
      date_published = "no date",
      identifier = "no identifier",
      citation_count = "no citations")

  } else {

    data.frame(
      r4r_id = x$persistent_identifier,
      name = x$recent_articles[[1]]$name,
      date_published = as.character(x$recent_articles[[1]]$date_published),
      identifier = x$recent_articles[[1]]$identifier,
      citation_count = x$recent_articles[[1]]$citations_count)
  }
}



getResearcherAffiliations <- function(x){
  if (is.null(x$affiliations[[1]])) {

    data.frame(
      r4r_id = x$persistent_identifier,
      name = "no name",
      identifier = "no identifier",
      postal_address = "no postal address",
      start_year = "no start year",
      end_year = "no end year",
      score = "no score",
      publication_count = "no publication count")

  } else {

    data.frame(
      r4r_id = x$persistent_identifier,
      name = x$affiliations[[1]]$name,
      identifier = x$affiliations[[1]]$identifier,
      postal_address = x$affiliations[[1]]$postal_address,
      start_year = as.character(x$affiliations[[1]]$start_year),
      end_year = as.character(x$affiliations[[1]]$end_year),
      score = as.character(x$affiliations[[1]]$score),
      publication_count = x$affiliations[[1]]$publication_count
    )
  }
}

getResearcherCollaborators <- function(x){
  if (is.null(x$collaborators[[1]])) {

    data.frame(
      r4r_id = x$persistent_identifier,
      identifier = "no_identifier",
      name = "no name",
      collaboration_count = "no count")

  } else {

    data.frame(
      r4r_id = x$persistent_identifier,
      identifier = x$collaborators[[1]]$identifier,
      name = x$collaborators[[1]]$name,
      collaboration_count = x$collaborators[[1]]$collaboration_count)
  }
}


getResearcherMostCited <- function(x){
  if (is.null(x$most_cited_articles[[1]])) {

    data.frame(
      r4r_id = x$persistent_identifier,
      name = "no article",
      date_published = "no date",
      identifier = "no identifier",
      citation_count = "no citations")

  } else {

    data.frame(
      r4r_id = x$persistent_identifier,
      name = x$most_cited_articles[[1]]$name,
      date_published = as.character(x$most_cited_articles[[1]]$date_published),
      identifier = x$most_cited_articles[[1]]$identifier,
      citation_count = x$most_cited_articles[[1]]$citations_count)
  }
}

# rm_top10 <- lapply(researcher_docs_top10, getResearcherMostCited)
# rm_top10 <- as.data.frame(do.call(rbind, rm_top10))
# rm_top10$category <- "Top 10"
# rm_top10 <- rm_top10 |>
#   select(category, r4r_id, name, date_published, identifier, citation_count)

print("Hi Stephen!")
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

top_20_rand <- dbGetQuery(
  con,
  "SELECT id FROM distinctAuthor
  ORDER BY RANDOM()
  LIMIT 20"
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

# pull researcher docs ----
researcher_docs_top10 <- lapply(top_10$id, getResearcher)
researcher_docs_top10_60to70 <- lapply(top_10_60to70$id, getResearcher)
researcher_docs_top10_70to80 <- lapply(top_10_70to80$id, getResearcher)
researcher_docs_top10_80to90 <- lapply(top_10_80to90$id, getResearcher)
researcher_docs_top10_90to95 <- lapply(top_10_90to95$id, getResearcher)
researcher_docs_top10_95to100<- lapply(top_10_95to100$id, getResearcher)
researcher_docs_top_10_collab <- lapply(top_10_collab$id, getResearcher)
researcher_docs_top_10_affiliations <- lapply(top_10_affiliations$source, getResearcher)
researcher_docs_top_20_rand <- lapply(top_20_rand$id, getResearcher)

# format and save core researcher data ----
rc_top10 <- lapply(researcher_docs_top10, getResearcherCore, y= "Top 10")
rc_top10 <- as.data.frame(do.call(rbind, rc_top10))
rc_top_10_60to70 <- lapply(researcher_docs_top10_60to70, getResearcherCore, y= "DoB 60 to 70")
rc_top_10_60to70 <- as.data.frame(do.call(rbind, rc_top_10_60to70))
rc_top10_70to80 <- lapply(researcher_docs_top10_70to80, getResearcherCore, y= "DoB 70 to 80")
rc_top10_70to80 <- as.data.frame(do.call(rbind, rc_top10_70to80))
rc_top10_80to90 <- lapply(researcher_docs_top10_80to90, getResearcherCore, y= "DoB 80 to 90")
rc_top10_80to90 <- as.data.frame(do.call(rbind, rc_top10_80to90))
rc_top10_90to95 <- lapply(researcher_docs_top10_90to95, getResearcherCore, y= "DoB 90 to 95")
rc_top10_90to95 <- as.data.frame(do.call(rbind, rc_top10_90to95))
rc_top10_95to100 <- lapply(researcher_docs_top10_95to100, getResearcherCore, y= "DoB 95 to 100")
rc_top10_95to100 <- as.data.frame(do.call(rbind, rc_top10_95to100))
rc_top_10_collab <- lapply(researcher_docs_top_10_collab, getResearcherCore, y= "Top 10 Collab")
rc_top_10_collab <- as.data.frame(do.call(rbind, rc_top_10_collab))
rc_top_10_affiliations <- lapply(researcher_docs_top_10_affiliations, getResearcherCore, y= "Top 10 Affil")
rc_top_10_affiliations <- as.data.frame(do.call(rbind, rc_top_10_affiliations))
rc_top_20_rand <- lapply(researcher_docs_top_20_rand, getResearcherCore, y= "Top 20 Random")
rc_top_20_rand <- as.data.frame(do.call(rbind, rc_top_20_rand))

rc <- rbind(
  rc_top10,
  rc_top_10_60to70,
  rc_top10_70to80,
  rc_top10_80to90,
  rc_top10_90to95,
  rc_top10_95to100,
  rc_top_10_collab,
  rc_top_10_affiliations,
  rc_top_20_rand
)

write.csv(rc, "app/r4r_researcher_review/rc.csv", row.names = FALSE)

# format and save researcher identifier data ----
ri_top10 <- lapply(researcher_docs_top10, getResearcherIdentifiers)
ri_top10 <- as.data.frame(do.call(rbind, ri_top10))
ri_top10$category <- "Top 10"
ri_top10 <- ri_top10 |>
  filter(identifiers != "") |>
  select(category, r4r_id, identifiers)

ri_top_10_60to70 <- lapply(researcher_docs_top10_60to70, getResearcherIdentifiers)
ri_top_10_60to70 <- as.data.frame(do.call(rbind, ri_top_10_60to70))
ri_top_10_60to70$category <- "DoB 60 to 70"
ri_top_10_60to70 <- ri_top_10_60to70 |>
  filter(identifiers != "") |>
  select(category, r4r_id, identifiers)

ri_top10_70to80 <- lapply(researcher_docs_top10_70to80, getResearcherIdentifiers)
ri_top10_70to80 <- as.data.frame(do.call(rbind, ri_top10_70to80))
ri_top10_70to80$category <- "DoB 70 to 80"
ri_top10_70to80 <- ri_top10_70to80 |>
  filter(identifiers != "") |>
  select(category, r4r_id, identifiers)

ri_top10_80to90 <- lapply(researcher_docs_top10_80to90, getResearcherIdentifiers)
ri_top10_80to90 <- as.data.frame(do.call(rbind, ri_top10_80to90))
ri_top10_80to90$category <- "DoB 80 to 90"
ri_top10_80to90 <- ri_top10_80to90 |>
  filter(identifiers != "") |>
  select(category, r4r_id, identifiers)

ri_top10_90to95 <- lapply(researcher_docs_top10_90to95, getResearcherIdentifiers)
ri_top10_90to95 <- as.data.frame(do.call(rbind, ri_top10_90to95))
ri_top10_90to95$category <- "DoB 90 to 95"
ri_top10_90to95 <- ri_top10_90to95 |>
  filter(identifiers != "") |>
  select(category, r4r_id, identifiers)

ri_top10_95to100 <- lapply(researcher_docs_top10_95to100, getResearcherIdentifiers)
ri_top10_95to100 <- as.data.frame(do.call(rbind, ri_top10_95to100))
ri_top10_95to100$category <- "DoB 95 to 100"
ri_top10_95to100 <- ri_top10_95to100 |>
  filter(identifiers != "") |>
  select(category, r4r_id, identifiers)

ri_top_10_collab <- lapply(researcher_docs_top_10_collab, getResearcherIdentifiers)
ri_top_10_collab <- as.data.frame(do.call(rbind, ri_top_10_collab))
ri_top_10_collab$category <- "Top 10 Collab"
ri_top_10_collab <- ri_top_10_collab |>
  filter(identifiers != "") |>
  select(category, r4r_id, identifiers)

ri_top_10_affiliations <- lapply(researcher_docs_top_10_affiliations, getResearcherIdentifiers)
ri_top_10_affiliations <- as.data.frame(do.call(rbind, ri_top_10_affiliations))
ri_top_10_affiliations$category <- "Top 10 Affil"
ri_top_10_affiliations <- ri_top_10_affiliations |>
  filter(identifiers != "") |>
  select(category, r4r_id, identifiers)

ri_top_20_rand <- lapply(researcher_docs_top_20_rand, getResearcherIdentifiers)
ri_top_20_rand <- as.data.frame(do.call(rbind, ri_top_20_rand))
ri_top_20_rand$category <- "Top 20 Random"
ri_top_20_rand <- ri_top_20_rand |>
  filter(identifiers != "") |>
  select(category, r4r_id, identifiers)

ri <- rbind(
  ri_top10,
  ri_top_10_60to70,
  ri_top10_70to80,
  ri_top10_80to90,
  ri_top10_90to95,
  ri_top10_95to100,
  ri_top_10_collab,
  ri_top_10_affiliations,
  ri_top_20_rand
)

write.csv(ri, "app/r4r_researcher_review/ri.csv", row.names = FALSE)

# format and save researcher name variants data ----
rn_top10 <- lapply(researcher_docs_top10, getResearcherNameVar)
rn_top10 <- as.data.frame(do.call(rbind, rn_top10))
rn_top10$category <- "Top 10"
rn_top10 <- rn_top10 |>
  select(category, r4r_id, alt_name, work_count)

rn_top_10_60to70 <- lapply(researcher_docs_top10_60to70, getResearcherNameVar)
rn_top_10_60to70 <- as.data.frame(do.call(rbind, rn_top_10_60to70))
rn_top_10_60to70$category <- "DoB 60 to 70"
rn_top_10_60to70 <- rn_top_10_60to70 |>
  select(category, r4r_id, alt_name, work_count)

rn_top10_70to80 <- lapply(researcher_docs_top10_70to80, getResearcherNameVar)
rn_top10_70to80 <- as.data.frame(do.call(rbind, rn_top10_70to80))
rn_top10_70to80$category <- "DoB 70 to 80"
rn_top10_70to80 <- rn_top10_70to80 |>
  select(category, r4r_id, alt_name, work_count)

rn_top10_80to90 <- lapply(researcher_docs_top10_80to90, getResearcherNameVar)
rn_top10_80to90 <- as.data.frame(do.call(rbind, rn_top10_80to90))
rn_top10_80to90$category <- "DoB 80 to 90"
rn_top10_80to90 <- rn_top10_80to90 |>
  select(category, r4r_id, alt_name, work_count)

rn_top10_90to95 <- lapply(researcher_docs_top10_90to95, getResearcherNameVar)
rn_top10_90to95 <- as.data.frame(do.call(rbind, rn_top10_90to95))
rn_top10_90to95$category <- "DoB 90 to 95"
rn_top10_90to95 <- rn_top10_90to95 |>
  select(category, r4r_id, alt_name, work_count)

rn_top10_95to100 <- lapply(researcher_docs_top10_95to100, getResearcherNameVar)
rn_top10_95to100 <- as.data.frame(do.call(rbind, rn_top10_95to100))
rn_top10_95to100$category <- "DoB 95 to 100"
rn_top10_95to100 <- rn_top10_95to100 |>
  select(category, r4r_id, alt_name, work_count)

rn_top_10_collab <- lapply(researcher_docs_top_10_collab, getResearcherNameVar)
rn_top_10_collab <- as.data.frame(do.call(rbind, rn_top_10_collab))
rn_top_10_collab$category <- "Top 10 Collab"
rn_top_10_collab <- rn_top_10_collab |>
  select(category, r4r_id, alt_name, work_count)

rn_top_10_affiliations <- lapply(researcher_docs_top_10_affiliations, getResearcherNameVar)
rn_top_10_affiliations <- as.data.frame(do.call(rbind, rn_top_10_affiliations))
rn_top_10_affiliations$category <- "Top 10 Affil"
rn_top_10_affiliations <- rn_top_10_affiliations |>
  select(category, r4r_id, alt_name, work_count)

rn_top_20_rand <- lapply(researcher_docs_top_20_rand, getResearcherNameVar)
rn_top_20_rand <- as.data.frame(do.call(rbind, rn_top_20_rand))
rn_top_20_rand$category <- "Top 20 Random"
rn_top_20_rand <- rn_top_20_rand |>
  select(category, r4r_id, alt_name, work_count)

rn <- rbind(
  rn_top10,
  rn_top_10_60to70,
  rn_top10_70to80,
  rn_top10_80to90,
  rn_top10_90to95,
  rn_top10_95to100,
  rn_top_10_collab,
  rn_top_10_affiliations,
  rn_top_20_rand
)

write.csv(rn, "app/r4r_researcher_review/rn.csv", row.names = FALSE)

# format and save researcher recent articles data ----
ra_top10 <- lapply(researcher_docs_top10, getResearcherArticles)
ra_top10 <- as.data.frame(do.call(rbind, ra_top10))
ra_top10$category <- "Top 10"
ra_top10 <- ra_top10 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

ra_top_10_60to70 <- lapply(researcher_docs_top10_60to70, getResearcherArticles)
ra_top_10_60to70 <- as.data.frame(do.call(rbind, ra_top_10_60to70))
ra_top_10_60to70$category <- "DoB 60 to 70"
ra_top_10_60to70 <- ra_top_10_60to70 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

ra_top10_70to80 <- lapply(researcher_docs_top10_70to80, getResearcherArticles)
ra_top10_70to80 <- as.data.frame(do.call(rbind, ra_top10_70to80))
ra_top10_70to80$category <- "DoB 70 to 80"
ra_top10_70to80 <- ra_top10_70to80 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

ra_top10_80to90 <- lapply(researcher_docs_top10_80to90, getResearcherArticles)
ra_top10_80to90 <- as.data.frame(do.call(rbind, ra_top10_80to90))
ra_top10_80to90$category <- "DoB 80 to 90"
ra_top10_70to80 <- ra_top10_70to80 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

ra_top10_90to95 <- lapply(researcher_docs_top10_90to95, getResearcherArticles)
ra_top10_90to95 <- as.data.frame(do.call(rbind, ra_top10_90to95))
ra_top10_90to95$category <- "DoB 90 to 95"
ra_top10_90to95 <- ra_top10_90to95 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

ra_top10_95to100 <- lapply(researcher_docs_top10_95to100, getResearcherArticles)
ra_top10_95to100 <- as.data.frame(do.call(rbind, ra_top10_95to100))
ra_top10_95to100$category <- "DoB 95 to 100"
ra_top10_95to100 <- ra_top10_95to100 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

ra_top_10_collab <- lapply(researcher_docs_top_10_collab, getResearcherArticles)
ra_top_10_collab <- as.data.frame(do.call(rbind, ra_top_10_collab))
ra_top_10_collab$category <- "Top 10 Collab"
ra_top_10_collab <- ra_top_10_collab |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

ra_top_10_affiliations <- lapply(researcher_docs_top_10_affiliations, getResearcherArticles)
ra_top_10_affiliations <- as.data.frame(do.call(rbind, ra_top_10_affiliations))
ra_top_10_affiliations$category <- "Top 10 Affil"
ra_top_10_affiliations <- ra_top_10_affiliations |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

ra_top_20_rand <- lapply(researcher_docs_top_20_rand, getResearcherArticles)
ra_top_20_rand <- as.data.frame(do.call(rbind, ra_top_20_rand))
ra_top_20_rand$category <- "Top 20 Random"
ra_top_20_rand <- ra_top_20_rand |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

ra <- rbind(
  ra_top10,
  ra_top_10_60to70,
  ra_top10_70to80,
  ra_top10_80to90,
  ra_top10_90to95,
  ra_top10_95to100,
  ra_top_10_collab,
  ra_top_10_affiliations,
  ra_top_20_rand
)

write.csv(ra, "app/r4r_researcher_review/ra.csv", row.names = FALSE)

# format and save researcher most cited articles data ----
rm_top10 <- lapply(researcher_docs_top10, getResearcherMostCited)
rm_top10 <- as.data.frame(do.call(rbind, rm_top10))
rm_top10$category <- "Top 10"
rm_top10 <- rm_top10 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

rm_top_10_60to70 <- lapply(researcher_docs_top10_60to70, getResearcherMostCited)
rm_top_10_60to70 <- as.data.frame(do.call(rbind, rm_top_10_60to70))
rm_top_10_60to70$category <- "DoB 60 to 70"
rm_top_10_60to70 <- rm_top_10_60to70 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

rm_top10_70to80 <- lapply(researcher_docs_top10_70to80, getResearcherMostCited)
rm_top10_70to80 <- as.data.frame(do.call(rbind, rm_top10_70to80))
rm_top10_70to80$category <- "DoB 70 to 80"
rm_top10_70to80 <- rm_top10_70to80 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

rm_top10_80to90 <- lapply(researcher_docs_top10_80to90, getResearcherMostCited)
rm_top10_80to90 <- as.data.frame(do.call(rbind, rm_top10_80to90))
rm_top10_80to90$category <- "DoB 80 to 90"
rm_top10_80to90 <- rm_top10_80to90 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

rm_top10_90to95 <- lapply(researcher_docs_top10_90to95, getResearcherMostCited)
rm_top10_90to95 <- as.data.frame(do.call(rbind, rm_top10_90to95))
rm_top10_90to95$category <- "DoB 90 to 95"
rm_top10_90to95 <- rm_top10_90to95 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

rm_top10_95to100 <- lapply(researcher_docs_top10_95to100, getResearcherMostCited)
rm_top10_95to100 <- as.data.frame(do.call(rbind, rm_top10_95to100))
rm_top10_95to100$category <- "DoB 95 to 100"
rm_top10_95to100 <- rm_top10_95to100 |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

rm_top_10_collab <- lapply(researcher_docs_top_10_collab, getResearcherMostCited)
rm_top_10_collab <- as.data.frame(do.call(rbind, rm_top_10_collab))
rm_top_10_collab$category <- "Top 10 Collab"
rm_top_10_collab <- rm_top_10_collab |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

rm_top_10_affiliations <- lapply(researcher_docs_top_10_affiliations, getResearcherMostCited)
rm_top_10_affiliations <- as.data.frame(do.call(rbind, rm_top_10_affiliations))
rm_top_10_affiliations$category <- "Top 10 Affil"
rm_top_10_affiliations <- rm_top_10_affiliations |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

rm_top_20_rand <- lapply(researcher_docs_top_20_rand, getResearcherMostCited)
rm_top_20_rand <- as.data.frame(do.call(rbind, rm_top_20_rand))
rm_top_20_rand$category <- "Top 20 Random"
rm_top_20_rand <- rm_top_20_rand |>
  select(category, r4r_id, name, date_published, identifier, citation_count)

rm <- rbind(
  rm_top10,
  rm_top_10_60to70,
  rm_top10_70to80,
  rm_top10_80to90,
  rm_top10_90to95,
  rm_top10_95to100,
  rm_top_10_collab,
  rm_top_10_affiliations,
  rm_top_20_rand
)

write.csv(rm, "app/r4r_researcher_review/rm.csv", row.names = FALSE)

# format and save researcher affiliations data ----
rf_top10 <- lapply(researcher_docs_top10, getResearcherAffiliations)
rf_top10 <- as.data.frame(do.call(rbind, rf_top10))
rf_top10$category <- "Top 10"
rf_top10 <- rf_top10 |>
  select(category, r4r_id, name, identifier, postal_address, start_year, end_year, score, publication_count)

rf_top_10_60to70 <- lapply(researcher_docs_top10_60to70, getResearcherAffiliations)
rf_top_10_60to70 <- as.data.frame(do.call(rbind, rf_top_10_60to70))
rf_top_10_60to70$category <- "DoB 60 to 70"
rf_top_10_60to70 <- rf_top_10_60to70 |>
  select(category, r4r_id, name, identifier, postal_address, start_year, end_year, score, publication_count)

rf_top10_70to80 <- lapply(researcher_docs_top10_70to80, getResearcherAffiliations)
rf_top10_70to80 <- as.data.frame(do.call(rbind, rf_top10_70to80))
rf_top10_70to80$category <- "DoB 70 to 80"
rf_top10_70to80 <- rf_top10_70to80 |>
  select(category, r4r_id, name, identifier, postal_address, start_year, end_year, score, publication_count)

rf_top10_80to90 <- lapply(researcher_docs_top10_80to90, getResearcherAffiliations)
rf_top10_80to90 <- as.data.frame(do.call(rbind, rf_top10_80to90))
rf_top10_80to90$category <- "DoB 80 to 90"
rf_top10_80to90 <- rf_top10_80to90 |>
  select(category, r4r_id, name, identifier, postal_address, start_year, end_year, score, publication_count)

rf_top10_90to95 <- lapply(researcher_docs_top10_90to95, getResearcherAffiliations)
rf_top10_90to95 <- as.data.frame(do.call(rbind, rf_top10_90to95))
rf_top10_90to95$category <- "DoB 90 to 95"
rf_top10_90to95 <- rf_top10_90to95 |>
  select(category, r4r_id, name, identifier, postal_address, start_year, end_year, score, publication_count)

rf_top10_95to100 <- lapply(researcher_docs_top10_95to100, getResearcherAffiliations)
rf_top10_95to100 <- as.data.frame(do.call(rbind, rf_top10_95to100))
rf_top10_95to100$category <- "DoB 95 to 100"
rf_top10_95to100 <- rf_top10_95to100 |>
  select(category, r4r_id, name, identifier, postal_address, start_year, end_year, score, publication_count)

rf_top_10_collab <- lapply(researcher_docs_top_10_collab, getResearcherAffiliations)
rf_top_10_collab <- as.data.frame(do.call(rbind, rf_top_10_collab))
rf_top_10_collab$category <- "Top 10 Collab"
rf_top_10_collab <- rf_top_10_collab |>
  select(category, r4r_id, name, identifier, postal_address, start_year, end_year, score, publication_count)

rf_top_10_affiliations <- lapply(researcher_docs_top_10_affiliations, getResearcherAffiliations)
rf_top_10_affiliations <- as.data.frame(do.call(rbind, rf_top_10_affiliations))
rf_top_10_affiliations$category <- "Top 10 Affil"
rf_top_10_affiliations <- rf_top_10_affiliations |>
  select(category, r4r_id, name, identifier, postal_address, start_year, end_year, score, publication_count)

rf_top_20_rand <- lapply(researcher_docs_top_20_rand, getResearcherAffiliations)
rf_top_20_rand <- as.data.frame(do.call(rbind, rf_top_20_rand))
rf_top_20_rand$category <- "Top 20 Random"
rf_top_20_rand <- rf_top_20_rand |>
  select(category, r4r_id, name, identifier, postal_address, start_year, end_year, score, publication_count)

rf <- rbind(
  rf_top10,
  rf_top_10_60to70,
  rf_top10_70to80,
  rf_top10_80to90,
  rf_top10_90to95,
  rf_top10_95to100,
  rf_top_10_collab,
  rf_top_10_affiliations,
  rf_top_20_rand
)

write.csv(rf, "app/r4r_researcher_review/rf.csv", row.names = FALSE)

# format and save researcher collaborators data ----
rl_top10 <- lapply(researcher_docs_top10, getResearcherCollaborators)
rl_top10 <- as.data.frame(do.call(rbind, rl_top10))
rl_top10$category <- "Top 10"
rl_top10 <- rl_top10 |>
  select(category, r4r_id, name, identifier, collaboration_count)

rl_top_10_60to70 <- lapply(researcher_docs_top10_60to70, getResearcherCollaborators)
rl_top_10_60to70 <- as.data.frame(do.call(rbind, rl_top_10_60to70))
rl_top_10_60to70$category <- "DoB 60 to 70"
rl_top_10_60to70 <- rl_top_10_60to70 |>
  select(category, r4r_id, name, identifier, collaboration_count)

rl_top10_70to80 <- lapply(researcher_docs_top10_70to80, getResearcherCollaborators)
rl_top10_70to80 <- as.data.frame(do.call(rbind, rl_top10_70to80))
rl_top10_70to80$category <- "DoB 70 to 80"
rl_top10_70to80 <- rl_top10_70to80 |>
  select(category, r4r_id, name, identifier, collaboration_count)

rl_top10_80to90 <- lapply(researcher_docs_top10_80to90, getResearcherCollaborators)
rl_top10_80to90 <- as.data.frame(do.call(rbind, rl_top10_80to90))
rl_top10_80to90$category <- "DoB 80 to 90"
rl_top10_80to90 <- rl_top10_80to90 |>
  select(category, r4r_id, name, identifier, collaboration_count)

rl_top10_90to95 <- lapply(researcher_docs_top10_90to95, getResearcherCollaborators)
rl_top10_90to95 <- as.data.frame(do.call(rbind, rl_top10_90to95))
rl_top10_90to95$category <- "DoB 90 to 95"
rl_top10_90to95 <- rl_top10_90to95 |>
  select(category, r4r_id, name, identifier, collaboration_count)

rl_top10_95to100 <- lapply(researcher_docs_top10_95to100, getResearcherCollaborators)
rl_top10_95to100 <- as.data.frame(do.call(rbind, rl_top10_95to100))
rl_top10_95to100$category <- "DoB 95 to 100"
rl_top10_95to100 <- rl_top10_95to100 |>
  select(category, r4r_id, name, identifier, collaboration_count)

rl_top_10_collab <- lapply(researcher_docs_top_10_collab, getResearcherCollaborators)
rl_top_10_collab <- as.data.frame(do.call(rbind, rl_top_10_collab))
rl_top_10_collab$category <- "Top 10 Collab"
rl_top_10_collab <- rl_top_10_collab |>
  select(category, r4r_id, name, identifier, collaboration_count)

rl_top_10_affiliations <- lapply(researcher_docs_top_10_affiliations, getResearcherCollaborators)
rl_top_10_affiliations <- as.data.frame(do.call(rbind, rl_top_10_affiliations))
rl_top_10_affiliations$category <- "Top 10 Affil"
rl_top_10_affiliations <- rl_top_10_affiliations |>
  select(category, r4r_id, name, identifier, collaboration_count)

rl_top_20_rand <- lapply(researcher_docs_top_20_rand, getResearcherCollaborators)
rl_top_20_rand <- as.data.frame(do.call(rbind, rl_top_20_rand))
rl_top_20_rand$category <- "Top 20 Random"
rl_top_20_rand <- rl_top_20_rand |>
  select(category, r4r_id, name, identifier, collaboration_count)

rl <- rbind(
  rl_top10,
  rl_top_10_60to70,
  rl_top10_70to80,
  rl_top10_80to90,
  rl_top10_90to95,
  rl_top10_95to100,
  rl_top_10_collab,
  rl_top_10_affiliations,
  rl_top_20_rand
)

write.csv(rl, "app/r4r_researcher_review/rl.csv", row.names = FALSE)
