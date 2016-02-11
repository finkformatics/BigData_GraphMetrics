# BigData_GraphMetrics
The repository for the topic "Graph Metrics and Measures" of the Big Data Praktikum at the University of Leipzig.

## Visualization
[R](https://www.r-project.org) is used to create the graph metrics report.

### Install
Run these commands in the R shell to install dependencies:
```
install.packages('knitr', dependencies = TRUE)
install.packages('rjson')
```
[knitr](http://yihui.name/knitr/) create the HTML file for us with embedded R commands.

[rjson](https://cran.r-project.org/web/packages/rjson/index.html) reads the created JSON files with the graph metrics and builds a data frame object.

### Execute
Now run `r -f visualization/output.R` from the root folder and open `visualization/output.html` in your favoured. [Bootstrap](http://getbootstrap.com) provides the necessary styles thus an internet connection is needed.

### Optional
Besided the HTML file `visualization/rscripts` provides different R scripts to generate an adequate output.
