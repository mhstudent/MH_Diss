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

import com.google.gson.Gson;
import java.io.File;
import static java.lang.Boolean.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterResponse;
import twitter4j.User;
import twitter4j.json.DataObjectFactory;

/**
 *
 * @author benhowaga
 */
public class TwitterDb implements GraphDatabaseService {
// variables

    private GraphDatabaseService graphDb;

    //constructors
    private TwitterDb() {
    }
    
    public TwitterDb(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    public TwitterDb(File DB_FILE) {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_FILE);
    }

    // getters
    public GraphDatabaseService getGDS() {
        return graphDb;
    }

    public Node getNode(Status status) {
        Node node = null;
        try (Transaction tx = this.beginTx()) {
            node = this.findNode(Labels.STATUS, "ID", status.getId());
            tx.success();
            tx.close();
        }
        return node;
    }

    public Node getNode(User user) {
        return getUserNode(user.getId());
    }
    
    public Node getUserNode(long userId) {
        Node node = null;
                try (Transaction tx = this.beginTx()) {
            node = this.findNode(Labels.USER, "ID", userId);
            tx.success();
            tx.close();
        }
        return node;
    }

    public long existTweetsBefore(User user, Date sinceDate) {
        String col1 = "Created_at_L", col2 = "ID";
        String search = String.format("match (u:%s)-[:TWEETED]-(s:%s) WHERE u.ID=%d return s.%s, s.%S ORDER BY s.%s ASC LIMIT 1", Labels.USER, Labels.STATUS, user.getId(), col1, col2, col1);
        Result result = null;
        try (Transaction tx = this.beginTx()) {
            result = this.execute(search);
            tx.success();
            tx.close();
        }
        if (result.hasNext()) {
            Map<String, Object> row = result.next();
            Date minDate = new Date((long) row.get(result.columns().get(0)));
            long minId = (long) row.get(result.columns().get(1));
            if (minDate.after(sinceDate)) {
                return minId;
            } else {
                return -1L;
            }
        }
        return 0L;
    }

    public List<Node> getUsers(List<Status> statuses) {
        List<Node> usrList = new ArrayList<>();
        try(Transaction tx = this.beginTx()) {
            for (Status status : statuses) {
                Node usrNode = this.findNode(Labels.USER,"ID",status.getUser().getId());
                usrList.add(usrNode);
            }
            tx.close();
        }
        return usrList;
    }
    
    // mutators
    public Node add(Status status) {
        List<Status> statusList = new ArrayList<>();
        statusList.add(status);
        List<Node> nodes = this.addStatuses(statusList);
        Node node = nodes.get(0);
        return node;
    }

    public Node addRaw(Status status) {
        Node node = this.createNode(Labels.STATUS);
        node.setProperty("ID", status.getId());
        node.setProperty("Text", status.getText());
        node.setProperty("FavCount", status.getFavoriteCount());
        node.setProperty("Created_at", status.getCreatedAt().toString());
        node.setProperty("Created_at_L", status.getCreatedAt().getTime());
        node.setProperty("Retweet", status.isRetweet());
        node.setProperty("RetweetCount", status.getRetweetCount());
        String json = new Gson().toJson(status);
        node.setProperty("JSON", json);
        return node;
    }

    public List<Node> addStatuses(List<Status> statuses) {
        List<Node> existingNodes = new ArrayList<>();
        List<Status> statusesNeeded = new ArrayList<>();
        List<Node> nodesAdded = new ArrayList<>();
        try (Transaction tx = this.beginTx()) {
            for (Status status : statuses) {
                Node node = graphDb.findNode(Labels.STATUS, "ID", status.getId());
                if (node != null) {
                    existingNodes.add(node);
                } else {
                    statusesNeeded.add(status);
                }
            }
            tx.close();
        }
        try (Transaction tx = this.beginTx()) {
            for (Status status : statusesNeeded) {
                Node node = this.addRaw(status);
                User user = status.getUser();
                Node usrNode = this.findNode(Labels.USER, "ID", user.getId());
                if (usrNode == null) {
                    usrNode = this.addRaw(user);
                }
                usrNode.createRelationshipTo(node, RelTypes.TWEETED);
                nodesAdded.add(node);
            }
            tx.success();
            tx.close();
        }
        existingNodes.addAll(nodesAdded);
        return existingNodes;
    }

    public Node add(User user) {
        List<User> userList = new ArrayList<>();
        userList.add(user);
        List<Node> nodes = this.addUsers(userList);
        Node node = nodes.get(0);
        return node;
    }

    private Node addRaw(User user) {
        Node node = this.createNode(Labels.USER);
        node.setProperty("ID", user.getId());
        node.setProperty("Followers", user.getFollowersCount());
        node.setProperty("Friends", user.getFriendsCount());
        node.setProperty("Location", user.getLocation());
        node.setProperty("Name", user.getName());
        node.setProperty("Screen Name", user.getScreenName());
//            node.setProperty("Timezone",user.getTimeZone());
        node.setProperty("Language", user.getLang());
        String json = new Gson().toJson(user);
        node.setProperty("JSON", json);
        return node;
    }

    public List<Node> addUsers(List<User> users) {
        List<Node> existingNodes = new ArrayList<>();
        List<User> usersNeeded = new ArrayList<>();
        List<Node> nodesAdded = new ArrayList<>();
        try (Transaction tx = this.beginTx()) {
            for (User user : users) {
                Node node = graphDb.findNode(Labels.USER, "ID", user.getId());
                if (node != null) {
                    existingNodes.add(node);
                } else {
                    usersNeeded.add(user);
                }
            }
            tx.close();
        }
        try (Transaction tx = this.beginTx()) {
            for (User user : usersNeeded) {
                Node node = this.addRaw(user);
                nodesAdded.add(node);
            }
            tx.success();
            tx.close();
        }
        existingNodes.addAll(nodesAdded);
        return existingNodes;
    }

    public void addLabels(List<Node> nodes, Label[] labels) {
        try (Transaction tx = this.beginTx()) {
            for (Node node : nodes) {
                for (Label label : labels) {
                    node.addLabel(label);
                }
            }
            tx.success();
            tx.close();
        }
    }
    
    public void addLabel(Node node, Label label) {
        List<Node> nodeList = new ArrayList<>();
        nodeList.add(node);
        Label[] labelList = new Label[]{label};
        this.addLabels(nodeList,labelList);
    }
    
    public void removeLabels(List<Node> nodes, Label[] labels) {
        try (Transaction tx = this.beginTx()) {
            for (Node node : nodes) {
                for (Label label : labels) {
                    node.removeLabel(label);
                }
            }
            tx.success();
            tx.close();
        }
    }
    
    public void removeLabel(Node node, Label label) {
        List<Node> nodeList = new ArrayList<>();
        nodeList.add(node);
        Label[] labelList = new Label[]{label};
        this.removeLabels(nodeList,labelList);
    }

    //enum
    enum RelTypes implements RelationshipType {
        TWEETED, REPLIED_TO, RETWEETED, FOLLOWS, PART_OF
    };

    public enum Labels implements Label {
        USER, STATUS, GROUP,
        SEEDUSER, MHUSER, HUBUSER, FARUSER,
        SEEDTWEET, MHTWEET, CTLTWEET,
        HYDRATED,UNHYDRATED,PARTIALLYHYDRATED,
        UNAVAILABLE
    }

    // inherited methods
    @Override
    public Node createNode() {
        return graphDb.createNode();
    }

    @Override
    public Node createNode(Label... labels) {
        return graphDb.createNode(labels);
    }

    @Override
    public Node getNodeById(long l) {
        return graphDb.getNodeById(l);
    }

    @Override
    public Relationship getRelationshipById(long l) {
        return graphDb.getRelationshipById(l);
    }

    @Override
    public ResourceIterable<Node> getAllNodes() {
        return graphDb.getAllNodes();
    }

    @Override
    public ResourceIterable<Relationship> getAllRelationships() {
        return graphDb.getAllRelationships();
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label, String string, Object o) {
        return graphDb.findNodes(label, string, o);
    }

    @Override
    public Node findNode(Label label, String string, Object o) {
        return graphDb.findNode(label, string, o);
    }

    @Override
    public ResourceIterator<Node> findNodes(Label label) {
        return graphDb.findNodes(label);
    }

    @Override
    public ResourceIterable<Label> getAllLabelsInUse() {
        return graphDb.getAllLabelsInUse();
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypesInUse() {
        return graphDb.getAllRelationshipTypesInUse();
    }

    @Override
    public ResourceIterable<Label> getAllLabels() {
        return graphDb.getAllLabels();
    }

    @Override
    public ResourceIterable<RelationshipType> getAllRelationshipTypes() {
        return graphDb.getAllRelationshipTypes();
    }

    @Override
    public ResourceIterable<String> getAllPropertyKeys() {
        return graphDb.getAllPropertyKeys();
    }

    @Override
    public boolean isAvailable(long l) {
        return graphDb.isAvailable(l);
    }

    @Override
    public void shutdown() {
        graphDb.shutdown();
    }

    @Override
    public Transaction beginTx() {
        return graphDb.beginTx();
    }

    @Override
    public Transaction beginTx(long l, TimeUnit tu) {
        return graphDb.beginTx(l, tu);
    }

    @Override
    public Result execute(String string) throws QueryExecutionException {
        return graphDb.execute(string);
    }

    @Override
    public Result execute(String string, long l, TimeUnit tu) throws QueryExecutionException {
        return graphDb.execute(string, l, tu);
    }

    @Override
    public Result execute(String string, Map<String, Object> map) throws QueryExecutionException {
        return graphDb.execute(string, map);
    }

    @Override
    public Result execute(String string, Map<String, Object> map, long l, TimeUnit tu) throws QueryExecutionException {
        return graphDb.execute(string, map, l, tu);
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(TransactionEventHandler<T> teh) {
        return graphDb.registerTransactionEventHandler(teh);
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(TransactionEventHandler<T> teh) {
        return graphDb.unregisterTransactionEventHandler(teh);
    }

    @Override
    public KernelEventHandler registerKernelEventHandler(KernelEventHandler keh) {
        return graphDb.registerKernelEventHandler(keh);
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler(KernelEventHandler keh) {
        return graphDb.unregisterKernelEventHandler(keh);
    }

    @Override
    public Schema schema() {
        return graphDb.schema();
    }

    @Override
    public IndexManager index() {
        return graphDb.index();
    }

    @Override
    public TraversalDescription traversalDescription() {
        return graphDb.traversalDescription();
    }

    @Override
    public BidirectionalTraversalDescription bidirectionalTraversalDescription() {
        return graphDb.bidirectionalTraversalDescription();
    }

}
