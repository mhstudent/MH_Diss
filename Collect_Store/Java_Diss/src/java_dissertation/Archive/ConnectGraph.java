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
package java_dissertation.Archive;

import java.io.File;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
//import org.neo4j.harness.junit.Neo4jRule;

/**
 *
 * @author benhowaga
 */
public class ConnectGraph {

    public static GraphDatabaseService connectGraph(String db_string) {
        File DB_FILE = new File(db_string);
        System.out.println(DB_FILE);
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(DB_FILE);
//        registerShutdownHook(graphDb);
        return graphDb;
    }

    private static void registerShutdownHook(GraphDatabaseService graphDb) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}