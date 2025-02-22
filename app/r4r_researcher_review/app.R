# https://www.davidsolito.com/post/conditional-drop-down-in-shiny/
# https://excelquick.com/r-shiny/selectinput-dependent-on-another-input/


# From ChatGPT
library(shiny)

# Define the available countries and their corresponding regions
country_regions <- list(
  "United States" = c("California", "Texas", "New York", "Florida", "Illinois"),
  "Canada" = c("Ontario", "Quebec", "British Columbia", "Alberta", "Manitoba"),
  "United Kingdom" = c("England", "Scotland", "Wales", "Northern Ireland"),
  "Australia" = c("New South Wales", "Victoria", "Queensland", "Western Australia", "South Australia"),
  "Germany" = c("Bavaria", "Berlin", "Hamburg", "Saxony", "Hesse")
)

# UI
ui <- fluidPage(
  titlePanel("Country and Region Selector"),

  sidebarLayout(
    sidebarPanel(
      selectInput("country", "Select a Country:",
                  choices = names(country_regions), selected = "United States"),

      selectInput("region", "Select a Region:", choices = NULL)
    ),

    mainPanel(
      h3("Selected Country and Region"),
      textOutput("selection")
    )
  )
)

# Server
server <- function(input, output, session) {

  # Update the region dropdown based on the selected country
  observeEvent(input$country, {
    updateSelectInput(session, "region", choices = country_regions[[input$country]])
  })

  # Display selected values
  output$selection <- renderText({
    paste("Country:", input$country, "| Region:", input$region)
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
