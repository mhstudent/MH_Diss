library(memisc)
library(reshape2)
library(matrixStats)

dataMH = read.csv("./tweetsMH.csv")
dataControl = read.csv("./tweetsnonMH.csv")
dataAll = merge(x=dataMH,y=dataControl,by="n.ID")

setwd("./GraphQueries")

dir()

head(dataAll)

par(mfrow=c(2,4))
for (col in names(dataMH)[c(2,5:10)])
{
    hist(dataAll[,paste0(col,".y")],main=col)
}

for (col in names(dataMH)[c(2,5:10)])
{
    print(col)
    print(codebook(dataAll[,paste0(col,".x")]))
    print(t.test(dataAll[,paste0(col,".x")])$conf.int)
    print(codebook(dataAll[,paste0(col,".y")]))
    print(t.test(dataAll[,paste0(col,".y")])$conf.int)    
}

for (col in names(dataMH)[5:8])
{
    print(col)
    print(t.test(dataAll[,paste0(col,".x")],dataAll[,paste0(col,".y")],paired=TRUE))
}

for (col in names(dataMH)c([9:10]))
{
    print(col)
    print(wilcox.test(dataAll[,paste0(col,".x")],dataAll[,paste0(col,".y")],paired=TRUE))
}
