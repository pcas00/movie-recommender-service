package edu.bc.casinepe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.util.*;

/**
 * Root resource (exposed at "movies" path)
 */
@Path("movies")
public class Movies {
    private static Logger logger = LogManager.getLogger(Movies.class.getName());
    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getMovies() {

        Map<Integer, List<Double>> movieRatings = parseMovieRatings("/mahoutRatings.dat");

        //Find highest average; TreeMap implementation sorts by value which will be average rating
        /*Map<Integer, Double> averageRatings = new TreeMap<Integer, Double>();
        for (Map.Entry<Integer, List<Double>> entry : movieRatings.entrySet()) {
            Integer movieId = entry.getKey();
            List<Double> allMovieRatings = entry.getValue();
            double averageRating = VectorOperations.mean(allMovieRatings);
            averageRatings.put(movieId, averageRating);

        } */
        StringBuilder sb = new StringBuilder();
        sb.append("Starting with movie rating size " + movieRatings.size() + "\n");
        Map<Integer, List<Double>> top5Movies =  topNFromMap(5, movieRatings);
        for (Map.Entry<Integer, List<Double>> entry : top5Movies.entrySet()) {
            sb.append("Movie: " + entry.getKey() + " with rating: " + entry.getValue() + "\n");
        }

        return sb.toString();

    }

    public Map<Integer, List<Double>> topNFromMap(int n, Map<Integer, List<Double>> map) {
        ValueComparator vc = new ValueComparator(map);
        TreeMap<Integer, List<Double>> mapSortedByValue = new TreeMap<Integer, List<Double>>(vc);
        mapSortedByValue.putAll(map);

        //Return top 5;
        Map<Integer, List<Double>> topNMap = new LinkedHashMap<Integer, List<Double>>();
        //StringBuilder sb = new StringBuilder();

        int i = 0;
        for (Map.Entry<Integer, List<Double>> entry : mapSortedByValue.entrySet()) {
            if (i > n) {
                break;
            }
            topNMap.put(entry.getKey(), entry.getValue());
            //sb.append(entry.getKey() + " : " + entry.getValue() + "\n");
            //sb.append(entry.getKey() + "\n");
            i++;
        }

        return topNMap;
    }

    /*
     * @param fileName relative file name string which must be located in resources/ directory
     * @return map of movie id's and respective ratings
     */
    public Map<Integer, List<Double>> parseMovieRatings(String fileName) {
        Scanner sc = null;
        Map<Integer, List<Double>> movieRatings = new HashMap<Integer, List<Double>>();

        try {
            sc = new Scanner(
                    new BufferedReader(
                            new FileReader(this.getClass().getResource(fileName)
                                    .getFile())));

            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                //Split into parts[0] => user id    parts[1] => movie id    parts[2] => rating

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
