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
Now run `r -f visualization/output.R` from the root folder and open `visualization/output.html` or `visualization/output_d3.html` in your favoured browser. [Bootstrap](http://getbootstrap.com) provides the necessary styles thus an internet connection is needed.
`visualization/output_d3.html` makes use of [d3](https://d3js.org) and [nvd3](http://nvd3.org) to provide a better overview.
An example output can be found [here](https://cdn.rawgit.com/tpohl90/c99f91bd13f2ee01afd2/raw/c859b75ecdd71d7948961ced77a472b71c9c36c1/output_d3.html).

### Optional
Besided the HTML file `visualization/rscripts` provides different R scripts to generate an adequate output.
