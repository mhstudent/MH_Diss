/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package java_dissertation.Snippets;

import static java.lang.Boolean.TRUE;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import static java_dissertation.Main_anon.TWITTER_CONSUMER_KEY;
import static java_dissertation.Main_anon.TWITTER_CONSUMER_SECRET;
// import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.ArrayUtils;
import twitter4j.*;
import twitter4j.auth.OAuth2Token;
import twitter4j.conf.ConfigurationBuilder;

/**
 *
 * @author benhowaga
 */
public class TwitterService {

    private TwitterFactory tf;
    private Twitter twitter;
    private static Map<String, RateLimitStatus> rateLimits;

    public TwitterService() {
    }

    /**
     * @throws twitter4j.TwitterException
     */
    private TwitterFactory getTwitterFactory() throws TwitterException {
        if (tf == null) {
            this.setTwitterFactory();
        }
        return tf;
    }

    public void start() {
        try {
            this.setTwitterFactory();
            this.setTwitterInstance();
//            System.out.println(twitter.getAuthorization()+"\n"+twitter.getConfiguration());
        } catch (TwitterException ex) {
            java.util.logging.Logger.getLogger(TwitterService.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public String getAuthorization() {
        return twitter.getAuthorization().toString();
    }

    private void setTwitterFactory() throws TwitterException {
        ConfigurationBuilder cb = new ConfigurationBuilder()
                .setJSONStoreEnabled(true)
                .setApplicationOnlyAuthEnabled(true)
                .setOAuthConsumerKey(TWITTER_CONSUMER_KEY)
                .setOAuthConsumerSecret(TWITTER_CONSUMER_SECRET);
//        tf = new TwitterFactory(cb.build());
//        System.out.println(TWITTER_CONSUMER_KEY + " : " + TWITTER_CONSUMER_SECRET);
        TwitterFactory temptf = new TwitterFactory(cb.build());
        OAuth2Token token = temptf.getInstance().getOAuth2Token();
        ConfigurationBuilder cb2 = new ConfigurationBuilder()
                .setJSONStoreEnabled(true)
                .setApplicationOnlyAuthEnabled(true)
                .setOAuthConsumerKey(TWITTER_CONSUMER_KEY)
                .setOAuthConsumerSecret(TWITTER_CONSUMER_SECRET)
                .setOAuth2TokenType(token.getTokenType())
                .setOAuth2AccessToken(token.getAccessToken());
        tf = new TwitterFactory(cb2.build());
    }

    private void setTwitterInstance() throws TwitterException {
        this.tf = getTwitterFactory();
        this.twitter = tf.getInstance();
//        this.twitter.setOAuth2Token(twitter.getOAuth2Token());
        this.updateRateLimits();
    }

    public synchronized void updateRateLimits() throws TwitterException {
        TwitterService.rateLimits = new HashMap<>(twitter.getRateLimitStatus("users", "statuses", "followers", "friends", "search"));
//        System.out.println(String.format("Rate limits:\n%s",rateLimits.toString()));
    }

    private void printRateLimits() {
        for (String key : rateLimits.keySet()) {
            System.out.println(key + " : " + rateLimits.get(key));
        }
    }

    public void setTwitterInstance(Twitter twitter) {
        this.twitter = twitter;
    }

    public void assertTwitterInstance() throws TwitterException {
        if (twitter == null) {
            this.setTwitterInstance();
        }
    }

    public synchronized void checkRateLimits(String... rateLimits) {
        for (String rateLimit : rateLimits) {
            RateLimitStatus currRLS = TwitterService.rateLimits.get(rateLimit);
            long waitTime = (((long) currRLS.getResetTimeInSeconds()) * 1000) - System.currentTimeMillis();
            if (waitTime >= 0 && currRLS.getRemaining() <= 0) {
                try {
                    Logger.getLogger(this.getClass().getName()).log(Level.INFO, String.format("Waiting for reset on %s: %d seconds", rateLimit, waitTime / 1000));
                    Thread.sleep(waitTime + 35000);
                    TwitterService.this.updateRateLimits();
                    Logger.getLogger(this.getClass().getName()).log(Level.INFO, "Restarting...");
                } catch (InterruptedException | TwitterException ex) {
                    Logger.getLogger(TwitterService.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
    }

    public class GetQuery implements Callable {

        private String search;

        public GetQuery(Twitter twitter, String search) {
            TwitterService.this.setTwitterInstance(twitter);
            this.search = search;
        }

        public GetQuery(String search) {
            try {
                TwitterService.this.assertTwitterInstance();
            } catch (TwitterException ex) {
                java.util.logging.Logger.getLogger(TwitterService.class.getName()).log(Level.SEVERE, null, ex);
            }
            this.search = search;
        }

        @Override
        public QueryResult call() {
//            System.out.println("Search for " + search);
            Query query = new Query(search);
            query.setCount(100);
            query.setLang("en");
            query.setResultType(Query.ResultType.recent);
            QueryResult result = null;
            TwitterService.this.checkRateLimits("/search/tweets");
            try {
                result = twitter.search(query);
                if (result.getRateLimitStatus() != null) {
                    TwitterService.rateLimits.replace("/search/tweets", result.getRateLimitStatus());
                }
//            String json_result = TwitterObjectFactory.getRawJSON(result);
            } catch (TwitterException ex) {
                java.util.logging.Logger.getLogger(TwitterService.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.out.println("Returning tweets...");
            return result;
        }

    }

    public class TweetsBefore implements Callable {

        private User user;
        private long minId;

        private TweetsBefore() {
        }

        public TweetsBefore(User user, long minId) {
            try {
                TwitterService.this.assertTwitterInstance();
            } catch (TwitterException ex) {
                java.util.logging.Logger.getLogger(TwitterService.class.getName()).log(Level.SEVERE, null, ex);
            }
            this.user = user;
            this.minId = minId;
        }

        @Override
        public ResponseList<Status> call() {
            ResponseList<Status> timeline = null;
            Paging paging = new Paging(1, 200);
            if (minId > 1) {
                paging.setMaxId(minId - 1);
            }
            TwitterService.this.checkRateLimits("/statuses/user_timeline");
            try {
                timeline = twitter.getUserTimeline(user.getId(), paging);
                rateLimits.put("/statuses/user_timeline", timeline.getRateLimitStatus());
            } catch (TwitterException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            }
            return timeline;
        }
    }
    
    public class GetUsers implements Callable {

        long[] idsToGet;

        public GetUsers() {
        }

        public GetUsers(long... id) {
            idsToGet = id;
        }

        public void setIds(long... id) {
            idsToGet = id;
        }

        public long[] getIds() {
            return idsToGet;
        }

        @Override
        public List<User> call() throws TwitterException {
            ResponseList<User> users = null;
            TwitterService.this.checkRateLimits("/users/lookup");
            try {
                users = twitter.lookupUsers(idsToGet);
                rateLimits.put("/users/lookup", users.getRateLimitStatus());
            } catch (TwitterException ex) {
                switch (ex.getErrorCode()) {
                    case 17:
                        throw ex;
                    default:
                        Logger.getLogger(TwitterService.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            return users;
        }

    }

    public class GetFriendIds implements Callable {

        private final int limit;
        private final long id;

        public GetFriendIds(long id) {
            this.id = id;
            this.limit = Default_Limits.FRIENDIDS;
        }

        public GetFriendIds(long id, int limit) {
            this.id = id;
            this.limit = limit;
        }

        @Override
        public List<Long> call(){
            long nextCursor = -1;
            List<Long> ids = new ArrayList<>();
            long[] newIds = null;
            long retrieved = 0;
            boolean cont = (limit == 0) ? TRUE : (retrieved < limit);
            while (nextCursor != 0 & cont) {
                // get next page of friends & update cursor
                TwitterService.this.checkRateLimits("/friends/ids");
                try {
                    IDs result = twitter.getFriendsIDs(id, nextCursor, 5000);
                    newIds = result.getIDs();
                    nextCursor = result.getNextCursor();
                    rateLimits.put("/users/lookup", result.getRateLimitStatus());
                    retrieved += result.getIDs().length;
                    cont = (limit == 0) ? TRUE : (retrieved < limit);
                } catch (TwitterException ex) {
                    switch (ex.getErrorCode()) {
                        case -1:
                            Logger.getLogger(DatabaseUser.class.getName()).log(Level.SEVERE, "User not available - may be suspended.", ex);
                        case 88:
                            Logger.getLogger(DatabaseUser.class.getName()).log(Level.SEVERE, "Rate limit exceeded.", ex);
                        default:
                            Logger.getLogger(DatabaseUser.class.getName()).log(Level.SEVERE, null, ex);
                    }

                }
                Long[] newIdsBoxed = ArrayUtils.toObject(newIds);
                List<Long> newIdsList = Arrays.asList(newIdsBoxed);
                ids.addAll(newIdsList);
            }
            return ids;

        }
    }

    private static class Default_Limits {

        static int FRIENDIDS = 3000;
    }

}
