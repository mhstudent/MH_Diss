/*
 * Copyright (C) 2017 benhowaga
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package java_dissertation.Snippets;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Integer.min;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.LongStream;
import java_dissertation.Snippets.TwitterDb.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import twitter4j.RateLimitStatus;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.URLEntity;
import twitter4j.User;

/**
 *
 * @author benhowaga
 */
public class DatabaseUser implements User {

    private User user;
    private long userTwitterId;
    private Node node;
    private boolean connected, friendsConnected, userAvailable;
    private TwitterService tService;

    private DatabaseUser() {
        this.connected = FALSE;
    }

    public DatabaseUser(long userId) {
        this.userAvailable = TRUE;
        this.userTwitterId = userId;
        this.connected = FALSE;
    }

    public DatabaseUser(User u, TwitterDb graphDb, TwitterService tService) {
        this.userAvailable = TRUE;
        this.connected = FALSE;
        user = u;
        userTwitterId = user.getId();
        this.connect(graphDb);
        this.tService = tService;
    }

    public User getUser() {
        return user;
    }

    public Node getNode() {
        return node;
    }

    private boolean isConnected() {
        return connected;
    }

    public void setUser() {
        assert (userTwitterId != 0L);
        setUser(userTwitterId);
    }

    public synchronized void setUser(long userId) {
        userTwitterId = userId;
        ExecutorService tExecutor = Executors.newSingleThreadExecutor();
        Future<List<User>> result = tExecutor.submit(tService.new GetUsers(userId));
        try {
            user = result.get().get(0);
        } catch (InterruptedException | ExecutionException ex) {
            userAvailable = FALSE;
            Logger.getLogger(DatabaseUser.class.getName()).log(Level.SEVERE, null, ex);
        }
        userAvailable = TRUE;
        tExecutor.shutdown();
    }

    public void setUser(User u) {
        user = u;
    }

    public void setNode(TwitterDb graphDb) {
        if (!connected) {
            connect(graphDb);
        } else if (node.getGraphDatabase() != graphDb.getGDS()) {
            connect(graphDb);
        }
    }

    public void setTwitterService(TwitterService tService) {
        this.tService = tService;
    }

    private void connect(TwitterDb graphDb) {
        if (user != null) {
            node = graphDb.add(user);
        } else {
            node = graphDb.getUserNode(userTwitterId);
        }
        if (node == null) {
            throw new RuntimeException("Unsuccessful. Either need a user or an existing node in the graph.");
        }
        try (Transaction tx = graphDb.beginTx()) {
            if (node.hasLabel(Labels.UNAVAILABLE)) {
                userAvailable = FALSE;
            }
            tx.close();
        }
        this.connected = TRUE;
    }

    public synchronized void addTimeline(Date sinceDate) throws ExecutionException, InterruptedException {
        assert connected;
        int maxiterations = 40;
        TwitterDb graphDb = new TwitterDb(node.getGraphDatabase());
        int iterations = 0;
        ExecutorService tExecutor = Executors.newSingleThreadExecutor();
        while (iterations < maxiterations) {
            Long minId = graphDb.existTweetsBefore(user, sinceDate);
            switch (minId.compareTo(new Long(0))) {
                case -1:
                    String exitMessage = "unspecified";
                    switch (minId.intValue()) {
                        case -1:
                            exitMessage = String.format("User %d has tweets before sinceDate", user.getId());
                    }
                    System.out.print(String.format("... timeline populated"));
                    tExecutor.shutdown();
                    return;
                default:
                    Future<ResponseList<Status>> timeline = tExecutor.submit(tService.new TweetsBefore(user, minId));
                    try {
                        graphDb.addStatuses(timeline.get());
                    } catch (ExecutionException | InterruptedException ex) {
//                        Logger.getLogger(DatabaseUser.class.getName()).log(Level.SEVERE, null, ex);
                        throw ex;
                    }
                    iterations++;
            }
        }
        System.out.print("... timeline populated. Some earlier tweets excluded");
        tExecutor.shutdown();
    }

    public synchronized void addFriends() throws RuntimeException, ExecutionException, InterruptedException {
        assert connected;
        TwitterDb graphDb = new TwitterDb(node.getGraphDatabase());
        int maxNewNodes = 1000;
        if (!userAvailable) {
            throw new RuntimeException("User not available. May have been suspended.");
        }
        ExecutorService tExecutor = Executors.newSingleThreadExecutor();
        ExecutorService gExecutor = Executors.newSingleThreadExecutor();
        Future<List<Long>> idsResult = tExecutor.submit(tService.new GetFriendIds(user.getId()));
        List<Long> ids;
        setNode(graphDb);
        try {
            ids = idsResult.get();
            System.out.print(String.format("Friends: %d\t", ids.size()));
            try (Transaction tx = graphDb.beginTx()) {
                if (ids.isEmpty()) {
                    node.setProperty("FriendIds", "N/A");
                    node.setProperty("FriendsPopulated", "N/A");
                    tExecutor.shutdown();
                    gExecutor.shutdown();
                    tx.success();
                    tx.close();
                    return;
                } else {
                    node.setProperty("FriendIds", ids.toString());
                    node.setProperty("FriendsPopulated", "FALSE");
                    tx.success();
                    tx.close();
                }
            }
        } catch (InterruptedException | ExecutionException ex) {
//            Logger.getLogger(DatabaseUser.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        }
        // connect existing nodes to main node
        if (!friendsConnected) {
            connectFriends();
        }
        // download new nodes
        List<String> friendIds = new ArrayList<>();
        try (Transaction tx = graphDb.beginTx()) {
            String str = (String) node.getProperty("MissingFriendIds", "[]");
            friendIds.addAll(Arrays.asList(str.substring(1, str.length() - 1).split(",")));
            friendIds.remove("");
            tx.close();
        }
        List<Node> newNodes = new ArrayList<>();
        List<Long> addedIds = new ArrayList<>();
        TwitterService.GetUsers userQuery = tService.new GetUsers();
        int maxiterations = 100;
        int iterations = 0;
        while (newNodes.size() < friendIds.size() & iterations < maxiterations) {
            LongStream subStream = friendIds.stream()
                    .skip(newNodes.size())
                    .limit(100)
                    .mapToLong(u -> new Long(u.trim()))
                    .sequential();
            userQuery.setIds(subStream.toArray());
//            System.out.println(Arrays.toString(userQuery.getIds()));
            subStream.close();
            Future<List<User>> usersResult = tExecutor.submit(userQuery);
            try {
                newNodes.addAll(graphDb.addUsers(usersResult.get()));
                usersResult.get().forEach((user) -> addedIds.add(user.getId()));
            } catch (InterruptedException | ExecutionException ex) {
                Logger.getLogger(DatabaseUser.class.getName()).log(Level.SEVERE, null, ex);
            }
            iterations++;
        }
        try (Transaction tx = graphDb.beginTx()) {
            for (Node newNode : newNodes) {
                node.createRelationshipTo(newNode, RelTypes.FOLLOWS);
            }
            graphDb.addLabels(newNodes, new Label[]{Labels.FARUSER});
            tx.success();
            tx.close();
        }
        friendIds.removeAll(addedIds);
        try (Transaction tx = graphDb.beginTx()) {
            node.setProperty("FriendsPopulated", "TRUE");
            node.setProperty("MissingFriends", friendIds.toString());
            if (iterations >= maxiterations) {
                node.addLabel(Labels.PARTIALLYHYDRATED);
            }
            tx.success();
            tx.close();
        }
        gExecutor.shutdown();
        tExecutor.shutdown();
    }

    private void connectFriends() {
        assert connected;
        String search;
        Result result;
        TwitterDb graphDb = new TwitterDb(node.getGraphDatabase());
        try (Transaction tx = graphDb.beginTx()) {
            String str = (String) node.getProperty("FriendIds", "[]");
            if ("[]".equals(str)) {
                throw new RuntimeException("No friendIDs available. User may be suspended.");
            }
            List<Long> friendIds = new ArrayList<>();
            Arrays.asList(str.substring(1, str.length() - 1).split(","))
                    .forEach((string) -> friendIds.add(new Long(string.trim())));
            // set up while loop
            int stepsize = 100;
            int newLimit = 1000;
            int iterations = 0;
            List<Long> existingLinkedIds = new ArrayList<>();
            search = String.format("MATCH (u:USER)<-[r:FOLLOWS]-(u2:USER) WHERE u2.ID = %s \n RETURN DISTINCT u.ID", user.getId());
            result = graphDb.execute(search);
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                Long rtnId = (Long) row.get(result.columns().get(0));
                existingLinkedIds.add(rtnId);
            }
            friendIds.removeAll(existingLinkedIds);
            List<Long> missingIds = new ArrayList<>();
            Map<Long, Node> existingNodes = new HashMap<>();
            // run while loop
            while ((missingIds.size() < newLimit) & (!friendIds.isEmpty())) {
                List<Long> subIds = new ArrayList<>(friendIds.subList(0, min(stepsize, friendIds.size())));
                friendIds.removeAll(subIds);
                search = String.format("MATCH (u:USER) WHERE u.ID IN %s\n"
                        + "OPTIONAL MATCH (u)<-[r:FOLLOWS]-(u2:USER) WHERE u2.ID = %s\n"
                        + "WITH u,u2\n"
                        + "WHERE u2 IS NULL \n"
                        + "RETURN u,u.ID", Arrays.toString(subIds.toArray()), user.getId());
                result = graphDb.execute(search);
                while (result.hasNext()) {
                    Map<String, Object> row = result.next();
                    Node rtnNode = (Node) row.get(result.columns().get(0));
                    Long rtnId = (Long) row.get(result.columns().get(1));
                    existingNodes.put(rtnId, rtnNode);
                    subIds.remove(rtnId);
                }
                missingIds.addAll(subIds);
            }

            System.out.print(String.format("Existing Links: %d\t", existingLinkedIds.size()));
            System.out.print(String.format("Unlinked existing nodes: %d\t", existingNodes.keySet().size()));
            System.out.print(String.format("Missing nodes: %d\n", missingIds.size()));
            node.setProperty("MissingFriendIds", Arrays.toString(missingIds.toArray()));
            for (Node friendNode : existingNodes.values()) {
                node.createRelationshipTo(friendNode, RelTypes.FOLLOWS);
            }
            tx.success();
            tx.close();
        } catch (RuntimeException ex) {
            try (Transaction tx = graphDb.beginTx()) {
                node.addLabel(Labels.UNAVAILABLE);
            }
            userAvailable = FALSE;
        }
        friendsConnected = TRUE;
    }

    @Override
    public long getId() {
        return user.getId();
    }

    @Override
    public String getName() {
        return user.getName();
    }

    @Override
    public String getScreenName() {
        return user.getScreenName();
    }

    @Override
    public String getLocation() {
        return user.getLocation();
    }

    @Override
    public String getDescription() {
        return user.getDescription();
    }

    @Override
    public boolean isContributorsEnabled() {
        return user.isContributorsEnabled();
    }

    @Override
    public String getProfileImageURL() {
        return user.getProfileImageURL();
    }

    @Override
    public String getBiggerProfileImageURL() {
        return user.getBiggerProfileImageURL();
    }

    @Override
    public String getMiniProfileImageURL() {
        return user.getMiniProfileImageURL();
    }

    @Override
    public String getOriginalProfileImageURL() {
        return user.getOriginalProfileImageURL();
    }

    @Override
    public String getProfileImageURLHttps() {
        return user.getProfileBackgroundImageUrlHttps();
    }

    @Override
    public String getBiggerProfileImageURLHttps() {
        return user.getBiggerProfileImageURLHttps();
    }

    @Override
    public String getMiniProfileImageURLHttps() {
        return user.getMiniProfileImageURLHttps();
    }

    @Override
    public String getOriginalProfileImageURLHttps() {
        return user.getOriginalProfileImageURLHttps();
    }

    @Override
    public boolean isDefaultProfileImage() {
        return user.isDefaultProfileImage();
    }

    @Override
    public String getURL() {
        return user.getURL();
    }

    @Override
    public boolean isProtected() {
        return user.isProtected();
    }

    @Override
    public int getFollowersCount() {
        return user.getFollowersCount();
    }

    @Override
    public Status getStatus() {
        return user.getStatus();
    }

    @Override
    public String getProfileBackgroundColor() {
        return user.getProfileBackgroundColor();
    }

    @Override
    public String getProfileTextColor() {
        return user.getProfileTextColor();
    }

    @Override
    public String getProfileLinkColor() {
        return user.getProfileLinkColor();
    }

    @Override
    public String getProfileSidebarFillColor() {
        return user.getProfileSidebarFillColor();
    }

    @Override
    public String getProfileSidebarBorderColor() {
        return user.getProfileSidebarBorderColor();
    }

    @Override
    public boolean isProfileUseBackgroundImage() {
        return user.isProfileUseBackgroundImage();
    }

    @Override
    public boolean isDefaultProfile() {
        return user.isDefaultProfile();
    }

    @Override
    public boolean isShowAllInlineMedia() {
        return user.isShowAllInlineMedia();
    }

    @Override
    public int getFriendsCount() {
        return user.getFriendsCount();
    }

    @Override
    public Date getCreatedAt() {
        return user.getCreatedAt();
    }

    @Override
    public int getFavouritesCount() {
        return user.getFavouritesCount();
    }

    @Override
    public int getUtcOffset() {
        return user.getUtcOffset();
    }

    @Override
    public String getTimeZone() {
        return user.getTimeZone();
    }

    @Override
    public String getProfileBackgroundImageURL() {
        return user.getProfileBackgroundImageURL();
    }

    @Override
    public String getProfileBackgroundImageUrlHttps() {
        return user.getProfileBackgroundImageUrlHttps();
    }

    @Override
    public String getProfileBannerURL() {
        return user.getProfileBannerURL();
    }

    @Override
    public String getProfileBannerRetinaURL() {
        return user.getProfileBannerRetinaURL();
    }

    @Override
    public String getProfileBannerIPadURL() {
        return user.getProfileBannerIPadURL();
    }

    @Override
    public String getProfileBannerIPadRetinaURL() {
        return user.getProfileBannerIPadRetinaURL();
    }

    @Override
    public String getProfileBannerMobileURL() {
        return user.getProfileBannerMobileURL();
    }

    @Override
    public String getProfileBannerMobileRetinaURL() {
        return user.getProfileBannerMobileRetinaURL();
    }

    @Override
    public boolean isProfileBackgroundTiled() {
        return user.isProfileBackgroundTiled();
    }

    @Override
    public String getLang() {
        return user.getLang();
    }

    @Override
    public int getStatusesCount() {
        return user.getStatusesCount();
    }

    @Override
    public boolean isGeoEnabled() {
        return user.isGeoEnabled();
    }

    @Override
    public boolean isVerified() {
        return user.isVerified();
    }

    @Override
    public boolean isTranslator() {
        return user.isTranslator();
    }

    @Override
    public int getListedCount() {
        return user.getListedCount();
    }

    @Override
    public boolean isFollowRequestSent() {
        return user.isFollowRequestSent();
    }

    @Override
    public URLEntity[] getDescriptionURLEntities() {
        return user.getDescriptionURLEntities();
    }

    @Override
    public URLEntity getURLEntity() {
        return user.getURLEntity();
    }

    @Override
    public String[] getWithheldInCountries() {
        return user.getWithheldInCountries();
    }

    @Override
    public int compareTo(User o) {
        return user.compareTo(o);
    }

    @Override
    public RateLimitStatus getRateLimitStatus() {
        return user.getRateLimitStatus();
    }

    @Override
    public int getAccessLevel() {
        return user.getAccessLevel();
    }

}
