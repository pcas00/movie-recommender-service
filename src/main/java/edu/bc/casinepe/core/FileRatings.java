package edu.bc.casinepe.core;

import edu.bc.casinepe.jdbc.JDBCDataModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.cf.taste.impl.similarity.precompute.MultithreadedBatchItemSimilarities;

import java.io.*;
import java.net.URL;
import java.util.*;

/**
 * Created by petercasinelli on 2/18/14.
 */
public class FileRatings {
    private static Logger logger = LogManager.getLogger(FileRatings.class.getName());
    public static File ratingsFile = null;

    public static File getRatingsFile() {
        if (ratingsFile == null) {
            logger.info("Ratings file is null; creating new file");
            try {
                InputStream input = FileRatings.class.getResourceAsStream("/1mm-ratings.csv");
                ratingsFile = new File("1mm-ratings.csv");
                OutputStream out = new FileOutputStream(ratingsFile);
                int read;
                byte[] bytes = new byte[1024];

                while ((read = input.read(bytes)) != -1) {
                    out.write(bytes, 0, read);
                }

            } catch (IOException e) {
                logger.error("IOException: ", e);
            } catch (Exception e) {
                logger.error("Exception: ", e);
            }
        }
        logger.info("Returning ratings file");
        return ratingsFile;
    }
    /*
    * @param fileName relative file name string which must be located in resources/ directory
    * @return map of movie id's and respective ratings
    */
    public static Map<Integer, List<Double>> parseMovieRatings(String fileName) {
        logger.info("Getting movies from " + fileName);
        Scanner sc = null;
        Map<Integer, List<Double>> movieRatings = new HashMap<Integer, List<Double>>();

        try {
            sc = new Scanner(
                    new BufferedReader(
                            new FileReader(FileRatings.class.getResource(fileName)
                                    .getFile())));

            logger.info("Scanner is " + sc);

            while (sc.hasNextLine()) {
                System.out.println("Another line");
                logger.info("Another line");
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
            logger.info("Error: " + e.getStackTrace());
            logger.error("Error: " + e.getMessage() + " " + e.getStackTrace());
            e.printStackTrace();
        }

        return movieRatings;
    }
}
