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
package java_dissertation;

import java_dissertation.Snippets.TwitterDb;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java_dissertation.Snippets.*;
import java_dissertation.Snippets.TwitterDb.Labels;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import twitter4j.QueryResult;
import twitter4j.TwitterException;

/**
 *
 * @author benhowaga
 */
public class Main_anon {

    public static String TWITTER_CONSUMER_KEY =  "<CONSUMER_KEY>";
    public static String TWITTER_CONSUMER_SECRET = "<CONSUMER_SECRET>";
    public static String DB_PATH = "<DB_PATH>";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        TwitterDb graphDb = new TwitterDb(new File(DB_PATH));
        TwitterService tService = new TwitterService();
        ExecutorService tExecutor = Executors.newSingleThreadExecutor();
        tService.start();
        /**
         * Get initials, and add to graphDb.
         */
        // get seed tweets and users
        String[] zaydmanStrings = {"abusive", "addict", "addiction", "alzheimers", "asylum", "autism", "bipolar", "bonkers", "brain damage", "brain dead", "breakdown", "coping", "crazy", "distressed", "distressing", "disturbed", "disturbing", "eating disorder", "escaped from an asylum", "few sandwiches short of a picnic basket", "freak", "gone in the head", "halfwit", "hallucinating", "hallucinations", "hand fed", "lived experience", "living with addiction", "living with alcoholism", "living with depression", "living with ptsd", "loony", "loony bin", "lunatic", "madness", "manic depression", "mass murderers", "mental", "mental health", "no-one upstairs", "not all there", "not quite there", "not the sharpest knife in the drawer", "numscull", "nutcase", "nuts", "nutter", "nutty as a fruitcake", "OCD", "off their rocker", "operational stress", "out of it", "psycho", "psychopath", "ptsd", "recovery", "retard", "schizo", "schizophrenia", "screw loose", "screwed", "self-control", "self-determination", "self-harm", "stressed", "suicide", "therapist", "therapy", "wheelchair jockey", "window licker", "you belong in a home", "demented", "depressed", "depression", "deranged", "difficulty learning", "dignity", "disabled", "handicapped", "head case", "hurting yourself", "in recovery", "insane", "intellectually challenged", "learning difficulties", "mental health challenges", "mental hospital", "mental illness", "mental institution", "mentally challenged", "mentally handicapped", "mentally ill", "padded cells", "paranoid", "pedophile", "perverted", "psychiatric", "psychiatric health", "psychiatrist", "shock syndrome", "sick in the head", "simpleton", "split personality", "stigma", "strait jacket", "stress"};
        String[] zaydmanHashtags = {"#abuse", "#addiction", "#alzheimers", "#anxiety", "#bipolar", "#bpd", "#Operationalstress", "#mhsm", "#trauma", "#spsm", "#alcoholism", "#depressed", "#depression", "#eatingdisorders", "#endthestigma", "#IAmStigmaFree", "#mentalhealth", "#pts", "#anxiety", "#therapy", "#endthestigma", "#AA", "#mentalhealthmatters", "#mentalhealthawareness", "#mentalillness", "#MH", "#nostigma", "#nostigmas", "#1SmallAct", "#psychology", "#mhchat", "#schizophrenia", "#ptsd", "#psychology", "#presspause", "#mentalhealthmatters", "#ocd", "#suicideprevention", "#therapy", "#trauma", "#WMHD2017", "#worldmentalhealthday"};
        String[] zaydmanOriginalStrings = {"#mentalhealth", "#depression", "#worldmentalhealthday", "#WMHD2015", "#nostigma", "#nostigmas", "#eatingdisorders", "#suicide", "#ptsd", "#mentalhealthawareness ", "#mentalillness", "#stopsuicide", "#IAmStigmaFree", "#suicideprevention", "#MH", "#addiction", "#bipolar", "#stigma"};
        String[] zaydmanOriginalHashtags = {"Mental health", "depression", "stigma", "eating disorder", "suicide", "ptsd", "mental illness", "addiction", "bipolar"};
        String[][] toDo = {zaydmanStrings, zaydmanHashtags};
        for (String[] list : toDo) {
            for (String string : list) {
                Future<QueryResult> result = tExecutor.submit(tService.new GetQuery(String.format("\"%s\" AND -filter:retweets", string)));
                try {
                    List<Node> nodes = graphDb.addStatuses(result.get().getTweets());
                    graphDb.addLabels(nodes, new Label[]{Labels.SEEDTWEET, Labels.MHTWEET});
                    List<Node> usrNodes = graphDb.getUsers(result.get().getTweets());
                    graphDb.addLabels(usrNodes, new Label[]{Labels.SEEDUSER, Labels.MHUSER, Labels.UNHYDRATED});
                } catch (InterruptedException | ExecutionException ex) {
                    Logger.getLogger(Main_anon.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        /**
         * This section is highly wasteful and bears looking at.
         */
        String search = String.format("MATCH (u:%s:%s) RETURN u.%s LIMIT %d", Labels.SEEDUSER, Labels.UNHYDRATED, "ID", 1500);
        List<Long> ids = new ArrayList<>();
        List<Long> missedIds = new ArrayList<>();
        try (Transaction tx = graphDb.beginTx()) {
            Result gResult = graphDb.execute(search);
            while (gResult.hasNext()) {
                Map<String, Object> row = gResult.next();
                Long id = (long) row.get(gResult.columns().get(0));
                ids.add(id);
            }
            tx.success();
            tx.close();
//            System.out.println(ids);
        }
        int number = 0;
        int total  = ids.size();
        for (long id : ids.subList(0, ids.size())) {
            if (number % 10 == 0) System.out.print(String.format("\n%s: %d/%d populated.\n",new Date(System.currentTimeMillis()),number,total));
            System.out.print(String.format("\nPopulating user ID: %d\n", id));
            DatabaseUser user = new DatabaseUser(id);
            user.setTwitterService(tService);
            user.setUser();
            user.setNode(graphDb);
            try {
                tService.updateRateLimits();
                user.addFriends();
                System.out.print("... friends added ");
                Date sinceDate = new GregorianCalendar(2017, 05, 01).getTime();
                user.addTimeline(sinceDate);
                graphDb.addLabel(user.getNode(), Labels.HYDRATED);
                graphDb.removeLabel(user.getNode(), Labels.UNHYDRATED);
                try (Transaction tx = graphDb.beginTx()) {
                    System.out.print(String.format("... exiting, with labels: " + user.getNode().getLabels().toString()));
                    tx.close();
                }
            } catch (RuntimeException | TwitterException | ExecutionException | InterruptedException ex) {
                missedIds.add(id);
                Logger.getLogger(Main_anon.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println("... unsuccessful: " + ex);
            }
            number++;
        }
        System.out.println("Missed IDs:\n" + Arrays.toString(missedIds.toArray()));
        graphDb.shutdown();
        tExecutor.shutdown();
    }
}
