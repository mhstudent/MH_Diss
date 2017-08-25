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

import java.io.*;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonStructure;

/**
 *
 * @author benhowaga
 */
public class ParseSearch {

    public static void parseSearch(File search_file) throws FileNotFoundException, IOException {
        try (FileReader in = new FileReader(search_file)) {
            JsonReader rdr = Json.createReader(in);
            JsonStructure obj = rdr.read();
            System.out.println(obj);
            
        }
    }
}
