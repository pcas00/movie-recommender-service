package edu.bc.casinepe;

import com.codahale.metrics.Timer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.sql.*;
import java.util.*;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Root resource (exposed at "movies" path)
 */
@Path("movies")
public class MoviesResource {
    private String username = "petercasinelli";
    private String password = "postgres";
    private static Logger logger = LogManager.getLogger(MoviesResource.class.getName());
    private final Timer fileResponses = MetricSystem.metrics.timer(name(MoviesResource.class, "fileResponses"));
    private final Timer dbResponses = MetricSystem.metrics.timer(name(MoviesResource.class, "dbResponses"));


    /**
     * Method handling HTTP GET requests. The returned object will be sent
     * to the client as "text/plain" media type.
     *
     * @return String that will be returned as a text/plain response.
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getMovies() {
        final Timer.Context context = fileResponses.time();

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

    @GET
    @Path("/db")
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getMoviesFromDB() {

        logger.error("Testing error");
        final Timer.Context context = dbResponses.time();
        Connection conn = null;
        Statement stmt = null;

        try {
            // Create container for movies
            MoviesBean movies = new MoviesBean();

            Connection con = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/movie_recommender",
                    username,
                    password);

            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT movies.id, movies.title, AVG(rating) - 2 / (|/COUNT(movie_id)) AS average_rating " +
                                             "FROM movie_ratings, movies " +
                                             "WHERE movies.id = movie_ratings.movie_id " +
                                             "GROUP BY movies.id " +
                                             "ORDER BY average_rating DESC " +
                                             "LIMIT 5");

            while (rs.next()) {
                String title = rs.getString("title");
                double averageRating = rs.getDouble("average_rating");
                int movieId = rs.getInt("id");
                MovieBean m = new MovieBean(movieId, title, averageRating);
                movies.addMovieBean(m);

            }

            return movies;

        } catch (SQLException e) {
            logger.error("SQL statement failed: " + e);
        } finally {
            context.stop();
        }

        return null;
    }

}
