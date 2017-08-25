library(memisc)
library(reshape2)
library(matrixStats)
opar <- par

dataMH = read.csv("./tweetTimesMH.csv")
dataControl = read.csv("./tweetTimesnonMH.csv")
wideMH=dcast(dataMH,n.ID~n2.CA_H)
wideControl=dcast(dataControl,n.ID~n2.CA_H)
wideMH[is.na(wideMH)] <- 0
wideMH$total <- apply(wideMH[,2:25],1,sum)
for (col in names(wideMH)[2:25])
{
    wideMH[,paste0(col,"p")]=wideMH[,col]/wideMH$total
}
wideControl[is.na(wideControl)] <- 0
wideControl$total <- apply(wideControl[,2:25],1,sum)
for (col in names(wideControl)[2:25])
{
    wideControl[,paste0(col,"p")]=wideControl[,col]/wideControl$total
}

wideAll=merge(x=wideMH,y=wideControl,by="n.ID")
for (col in names(wideControl)[2:25])
{
    wideAll[,paste0(col,"diff")]=wideAll[,paste0(col,"p.x")]-wideAll[,paste0(col,"p.y")]
}

                                        # non-matched graph
pdf("unmatched-tweettimes.pdf",height=3,width=3.7)
par(mar=c(5,5,1,1)+0.1)
plot(apply(wideMH[27:50],2,median),type="b",col=2,xlab="Hour of posting",ylab="Median post proportion")
par(new=T)
points(apply(wideControl[27:50],2,median),type="b",col=3)
legend(x="topleft",legend=c("Mental Health posts","Control posts"),fill=c(2,3))
dev.off()

# non-matched graph
plot(apply(wideMH[27:50],2,median),type="b",col=2)
par(new=T)
points(apply(wideControl[27:50],2,median),type="b",col=3)



head(wideAll)




names(wideAll)[100:123]
plot(apply(wideAll[100:123],2,mean))

pdf("matched-tweettimes.pdf",height=3,width=3.7)
par(mar=c(5,5,1,1)+0.1)
plot(apply(wideAll[100:123],2,median),xlab="Hour of posting",ylab="Median difference in posting\nproportion (MH-Control)")
abline(h=0,lty=2 )
dev.off()

                                        #$ filter out users with very few tweets
names(wideAll)[100:123]
par(opar)
plot(apply(wideAll[wideAll$total.x>5&wideAll$total.y>5,100:123],2,mean))
# points(apply(wideAll[wideAll$total.x>5&wideAll$total.y>5,100:123],2,quantile)["25%",])
plot(apply(wideAll[wideAll$total.x>5&wideAll$total.y>5,100:123],2,median))
abline(h=0)

apply(wideAll[wideAll$total.x>5&wideAll$total.y>2,100:123],2,quantile)["25%",]

plot(apply(wideAll[wideAll$total.x>5&wideAll$total.y>5,100:123],2,mean))

                                        # useless plot!
par(mfrow=c(4,6),pch=".",mar=c(1,1,1,1)+0.1)
for (col in names(wideControl)[27:50]) {
    plot(wideAll[,paste0(col,".x")],wideAll[,paste0(col,".y")],axes=T,xlab="",ylab="",frame.plot=T)
}

par(mfrow=c(4,6),pch=".",mar=c(1,1,1,1)+0.1)
for (col in names(wideControl)[2:25]) {
    hist(wideAll[,paste0(col,"diff")])
}

names(wideAll)

names(wideControl)[27]
names(wideControl)
head(wideAll)

hist(wideAll[wideAll$total.x<25|wideAll$total.x<25,"total.x"])
hist(wideAll[wideAll$total.y<25,"total.y"])


names(quantile(c(1,2,3,4,5,67,8,9,14)))
