library(memisc)

data = read.csv("./nFriendsnLinks.csv")

head(data)

                                        # initial stats
codebook(data)

                                        # test on full dataset
wilcox.test(data$n_friends,data$n_links)
                                        # test on dataset with limited friends
with(data[data$n_friends<5000,],wilcox.test(n_friends,n_links))


