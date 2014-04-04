package edu.bc.casinepe.core;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by petercasinelli on 2/18/14.
 */
public class FileRatings {
    /*
    * @param fileName relative file name string which must be located in resources/ directory
    * @return map of movie id's and respective ratings
    */
    public static Map<Integer, List<Double>> parseMovieRatings(String fileName) {
        Scanner sc = null;
        Map<Integer, List<Double>> movieRatings = new HashMap<Integer, List<Double>>();

        try {
            sc = new Scanner(
                    new BufferedReader(
                            new FileReader(FileRatings.class.getResource(fileName)
                                    .getFile())));

            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                //Split into parts[0] => user id    parts[1] => movie id    parts[2] => rating  parts[3] => timestamp

                String[] lineParts = line.split(",");
                String movieId = lineParts[1];
                String movieRating = lineParts[2];

                List<Double> previousRatings = movieRatings.get(Integer.parseInt(movieId));

                if (previousRatings == null) {
                    previousRatings = new LinkedList<Double>();
                    movieRatings.put(Integer.parseInt(movieId), previousRatings);
                }

                previousRatings.add(Double.parseDouble(movieRating));


            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return movieRatings;
    }
}
