package edu.bc.casinepe;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.util.*;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Root resource (exposed at "movies" path)
 */
@Path("movies")
public class MoviesResource {

    private static Logger logger = LogManager.getLogger(MoviesResource.class.getName());
    private final Timer responses = MetricSystem.metrics.timer(name(MoviesResource.class, "responses"));

    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getMovies() {
        final Timer.Context context = responses.time();

        try {
            MoviesBean movies = new MoviesBean();
            Map<Integer, List<Double>> movieRatings = parseMovieRatings("/mahoutRatings.dat");

            Map<Integer, List<Double>> top5Movies =  topNFromMap(5, movieRatings);
            for (Map.Entry<Integer, List<Double>> entry : top5Movies.entrySet()) {
                MovieBean m = new MovieBean();
                List<Double> ratings = entry.getValue();
                m.setId(entry.getKey());
                m.setRating(VectorOperations.mean(ratings));
                m.setRatingsCount(ratings.size());
                movies.addMovieBean(m);
            }

            return movies;
        } finally {
            context.stop();
        }


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
