package edu.bc.casinepe;

import com.codahale.metrics.Timer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.jdbc.PostgreSQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.postgresql.ds.PGPoolingDataSource;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.ws.rs.*;
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
    private final Timer getMoviesFromDb = MetricSystem.metrics.timer(name(MoviesResource.class, "getMoviesFromDb"));
    private final Timer getSimilarMoviesFromDb = MetricSystem.metrics.timer(name(MoviesResource.class, "getSimilarMoviesFromDb"));



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

        final Timer.Context context = getMoviesFromDb.time();
        Connection conn = null;
        Statement stmt = null;

        try {
            // Create container for movies
            MoviesBean movies = new MoviesBean();

            /*Connection con = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/movie_recommender",
                    username,
                    password);  */
            conn = PGDataSource.getDataSource().getConnection();

            stmt = conn.createStatement();
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

            rs.close();
            stmt.close();
            conn.close();

            return movies;

        } catch (SQLException e) {
            logger.error("SQL statement failed: " + e);
        } finally {
            context.stop();
        }

        return null;
    }

    @GET
    @Path("/similar/{movieId}/{number : (\\d?\\d?)}")

    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getSimilarMovies(@PathParam("movieId") int movieId, @DefaultValue("5") @PathParam("number") int number) {
        //Default to 5
        if (number <= 0) {
            number = 5;
        }

        //Only return a maximum of 10 similar items
        if (number > 10) {
            number = 10;
        }

        logger.info("Getting " + number + " similar movies to movie ID " + movieId);


        // Start timer
        final Timer.Context context = getSimilarMoviesFromDb.time();
        MoviesBean recommendedMovies = new MoviesBean();

        try {
            //Setting up a data source to manage DB connection pooling
            //PGConnectionPoolDataSource dataSource2 = new PGConnectionPoolDataSource();

            PGPoolingDataSource dataSource = PGDataSource.getDataSource();

            PostgreSQLJDBCDataModel dataModel = new PostgreSQLJDBCDataModel(dataSource,
                    "movie_ratings",
                    "user_id",
                    "movie_id",
                    "rating",
                    "timestamp");


            logger.info("Finding similar items for movie id " + movieId);
            //ItemSimilarity itemSimilarity = new PearsonCorrelationSimilarity(dataModel);
            ItemSimilarity itemSimilarity = new LogLikelihoodSimilarity(dataModel);
            ItemBasedRecommender recommender = new GenericItemBasedRecommender(dataModel, itemSimilarity);

            List<RecommendedItem> recommendations = recommender.mostSimilarItems(movieId, number);
            MovieApi movieApi = new MovieApi();
            for (RecommendedItem item : recommendations) {
                /*logger.info("Found similar item: " + item);
                MovieBean m = new MovieBean();
                m.setId((int) item.getItemID());
                m.setRating(item.getValue());*/
                MovieBean m = movieApi.getMovie((int)item.getItemID());
                recommendedMovies.addMovieBean(m);
            }
            logger.info("Similar items are: " + recommendations);
            //logger.info("MoviesBean is items are: " + recommendedMovies);



        } catch (Exception e) {
            logger.error("There was a taste error: " + e);
        } finally {
            context.stop();

            return recommendedMovies;
        }

    }

}
