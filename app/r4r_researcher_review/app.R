# https://www.davidsolito.com/post/conditional-drop-down-in-shiny/
# https://excelquick.com/r-shiny/selectinput-dependent-on-another-input/


# From ChatGPT
library(shiny)
library(dplyr)

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
      h4("Researcher Identifiers"),
      tableOutput("ri")
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

    data.frame(
      Field = "Identifiers",
      Value = ri_temp$identifiers
    )
  })
}

# Run the application
shinyApp(ui = ui, server = server)





# ui <- fluidPage(
#
#     # Application title
#     titlePanel("Old Faithful Geyser Data"),
#
#     # Sidebar with a slider input for number of bins
#     sidebarLayout(
#         sidebarPanel(
#             sliderInput("bins",
#                         "Number of bins:",
#                         min = 1,
#                         max = 50,
#                         value = 30)
#         ),
#
#         # Show a plot of the generated distribution
#         mainPanel(
#            plotOutput("distPlot")
#         )
#     )
# )
#
# # Define server logic required to draw a histogram
# server <- function(input, output) {
#
#     output$distPlot <- renderPlot({
#         # generate bins based on input$bins from ui.R
#         x    <- faithful[, 2]
#         bins <- seq(min(x), max(x), length.out = input$bins + 1)
#
#         # draw the histogram with the specified number of bins
#         hist(x, breaks = bins, col = 'darkgray', border = 'white',
#              xlab = 'Waiting time to next eruption (in mins)',
#              main = 'Histogram of waiting times')
#     })
# }

# # Run the application
# shinyApp(ui = ui, server = server)
