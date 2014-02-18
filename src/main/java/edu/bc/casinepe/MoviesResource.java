package edu.bc.casinepe;

import com.codahale.metrics.Timer;
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
            Map<Integer, List<Double>> movieRatings = FileRatings.parseMovieRatings("/mahoutRatings.dat");

            Map<Integer, List<Double>> top5Movies =  VectorOperations.topNFromMap(5, movieRatings);
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

}
