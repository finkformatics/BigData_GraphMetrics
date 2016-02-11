library("rjson")
library("ggplot2")
spreadLabels <- fromJSON(file="exampleInput/spread-labels.json")$spreadLabels

labels <- sapply(spreadLabels, function(x) x$label)
counts <- sapply(spreadLabels, function(x) x$count)

dat <- data.frame(labels = labels, counts = counts)
ggplot(data=dat, aes(x=labels,y=counts))+geom_bar(stat='identity')+coord_flip()
