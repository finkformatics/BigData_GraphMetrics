library("rjson")
library("ggplot2")
attributesCount <- fromJSON(file="exampleInput/spread-attributes.json")$attributesCount

attributes <- sapply(attributesCount, function(x) x$attribute)
counts <- sapply(attributesCount, function(x) x$count)

dat <- data.frame(labels = attributes, counts = counts)
ggplot(data=dat, aes(x=labels,y=counts))+geom_bar(stat='identity')+coord_flip()
