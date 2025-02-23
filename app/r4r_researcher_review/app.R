# https://www.davidsolito.com/post/conditional-drop-down-in-shiny/
# https://excelquick.com/r-shiny/selectinput-dependent-on-another-input/


# From ChatGPT
library(shiny)
library(dplyr)

# data config
rc <- read.csv("rc.csv")
menu_list <- list(
  "Top 10" = rc[rc$category == "Top 10", 2],
  "DoB 60 to 70" = rc[rc$category == "DoB 60 to 70", 2],
  "DoB 70 to 80" = rc[rc$category == "DoB 70 to 80", 2],
  "DoB 80 to 90" = rc[rc$category == "DoB 80 to 90", 2],
  "DoB 90 to 95" = rc[rc$category == "DoB 90 to 95", 2],
  "DoB 95 to 100" = rc[rc$category == "DoB 95 to 100", 2],
  "Top 10 Collab" = rc[rc$category == "Top 10 Collab", 2],
  "Top 10 Affil" = rc[rc$category == "Top 10 Affil", 2],
  "Top 20 Random" = rc[rc$category == "Top 20 Random", 2]
)

ri <- read.csv("ri.csv")
rn <- read.csv("rn.csv")
ra <- read.csv("ra.csv")
rf <- read.csv("rf.csv")
rl <- read.csv("rl.csv")
rm <- read.csv("rm.csv")


# UI
ui <- fluidPage(
  titlePanel("Ringgold|Researchers - Data Review"),

  sidebarLayout(
    sidebarPanel(
      selectInput("category", "Select a Category:",
                  choices = names(menu_list), selected = "Top 10"),

      selectInput("r4r_id", "Select a Researcher:", choices = NULL)
    ),

    mainPanel(
      h3("Data for Selected Researcher"),
      h4("Researcher Core Data"),
      tableOutput("rc"),
      h4("Researcher Name Variants"),
      tableOutput("rn"),
      h4("Researcher Identifiers"),
      tableOutput("ri"),
      h4("Researcher Recent Articles"),
      tableOutput("ra"),
      h4("Researcher Most Cited Articles"),
      tableOutput("rm"),
      h4("Researcher Affiliated Organizations"),
      tableOutput("rf"),
      h4("Researcher Collaborators"),
      tableOutput("rl")

    )
  )
)

# Server
server <- function(input, output, session) {

  # Update the researcher id dropdown based on the selected category
  observeEvent(input$category, {
    updateSelectInput(session, "r4r_id", choices = menu_list[[input$category]])
  })

  # Display rc ----
  output$rc <- renderTable({
    rc_temp <- rc |>
      filter(category == input$category &
               r4r_id == input$r4r_id)

    data.frame(
      Field = c("r4r_id",
                "family_name",
                "given_name",
                "degree_of_belief",
                "authorship_period_start",
                "authorship_period_end",
                "occurrences"),
      Value = c(rc_temp$r4r_id,
                rc_temp$family_name,
                rc_temp$given_name,
                rc_temp$degree_of_belief,
                rc_temp$authorship_period_start,
                rc_temp$authorship_period_end,
                rc_temp$occurrences)
    )
  })

  # Display ri ----
  output$ri <- renderTable({
    ri_temp <- ri |>
      filter(category == input$category &
               r4r_id == input$r4r_id)

    if (nrow(ri_temp) == 0) {
      data.frame(
        Field = "Identifiers",
        Value = "No external identifiers"
      )
    } else {

    data.frame(
      Field = "Identifiers",
      Value = ri_temp$identifiers)
    }
  }) # end of ri

  # Display rn ----
  output$rn <- renderTable({
    rn_temp <- rn |>
      filter(category == input$category &
               r4r_id == input$r4r_id)

    if (nrow(rn_temp) == 0) {
      data.frame(
        Field = c("Alternate Name",
                  "Work Count"),
        Value = c("No Alternate Name",
                  "No Work Count")
      )
    } else {
      rn_temp |>
        select(alt_name, work_count) |>
        rename("Alternate Name" = alt_name,
               "Work Count" = work_count)
    }

  }) # end of rn

  # Display ra ----
  output$ra <- renderTable({
    ra_temp <- ra |>
      filter(category == input$category &
               r4r_id == input$r4r_id)

    if (nrow(ra_temp) == 0) {
      data.frame(
        Field = c("Article Name",
                  "PubDate",
                  "Identifier",
                  "Citation Count"),
        Value = c("No Article Name",
                  "No PubDate",
                  "No Identifier",
                  "No Citation Count")
      )
    } else {
      ra_temp |>
        select(name, date_published, identifier, citation_count) |>
        rename("Article Name" = name,
               "PubDate" = date_published,
               "Identifier" = identifier,
               "Citation Count" = citation_count)
    }

  }) # end of ra

  # Display rm ----
  output$rm <- renderTable({
    rm_temp <- rm |>
      filter(category == input$category &
               r4r_id == input$r4r_id)

    if (nrow(rm_temp) == 0) {
      data.frame(
        Field = c("Article Name",
                  "PubDate",
                  "Identifier",
                  "Citation Count"),
        Value = c("No Article Name",
                  "No PubDate",
                  "No Identifier",
                  "No Citation Count")
      )
    } else {
      rm_temp |>
        select(name, date_published, identifier, citation_count) |>
        rename("Article Name" = name,
               "PubDate" = date_published,
               "Identifier" = identifier,
               "Citation Count" = citation_count)
    }

  }) # end of rm

  # Display rf ----
  output$rf <- renderTable({
    rf_temp <- rf |>
      filter(category == input$category &
               r4r_id == input$r4r_id)

    if (nrow(rf_temp) == 0) {
      data.frame(
        Field = c("Org Name",
                  "Identifier",
                  "Postal Address",
                  "Start Year",
                  "End Year",
                  "Score",
                  "Publication Count"),
        Value = c("No Org Name",
                  "No Identifier",
                  "No Postal Address",
                  "No Start Year",
                  "No End Year",
                  "No Score",
                  "No Publication Count")
      )
    } else {
      rf_temp |>
        select(name, identifier, postal_address, start_year, end_year, score, publication_count) |>
        rename("Org Name" = name,
               "Identifier" = identifier,
               "Postal Address" = postal_address,
               "Start Year" = start_year,
               "End Year" = end_year,
               "Score" = score,
               "Publication Count" = publication_count)
    }

  }) # end of rf

  # Display rl ----
  output$rl <- renderTable({
    rl_temp <- rl |>
      filter(category == input$category &
               r4r_id == input$r4r_id)

    if (nrow(rl_temp) == 0) {
      data.frame(
        Field = c("Name",
                  "Identifier",
                  "Collaboration Count"),
        Value = c("No Name",
                  "No Identifier",
                  "No Collaboration Count")
      )
    } else {
      rl_temp |>
        select(name, identifier, collaboration_count) |>
        rename("Name" = name,
               "Identifier" = identifier,
               "Collaboration Count" = collaboration_count)
    }

  }) # end of rl

} # end of server

# Run the application
shinyApp(ui = ui, server = server)
