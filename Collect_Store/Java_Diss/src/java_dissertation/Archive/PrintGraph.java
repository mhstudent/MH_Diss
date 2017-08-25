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

import java.util.Map;
import java.util.Map.Entry;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

/**
 *
 * @author benhowaga
 */
public class PrintGraph {

    public static void printGraph(GraphDatabaseService db, int max) {
        try (Transaction ignored = db.beginTx();
                Result result = db.execute("MATCH (t:Tweet) RETURN t, t.Text")) {
            int count = 0;
            while (result.hasNext() && count < max) {
                Map<String, Object> row = result.next();
                for (Entry<String, Object> column : row.entrySet()) {
                    System.out.println(column.getKey() + ": " + column.getValue() + "; ");
                }
            count+=1;
            }
        }
    }
}
