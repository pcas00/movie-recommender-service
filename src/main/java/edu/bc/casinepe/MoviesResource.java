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
import org.postgresql.ds.PGPoolingDataSource;

import javax.sql.DataSource;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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

    @GET
    @Path("/similar/{movieId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getSimilarMovies(@PathParam("movieId") int movieId) {

        //Setting up a data source to manage DB connection pooling
        PGPoolingDataSource dataSource = new PGPoolingDataSource();
        dataSource.setDataSourceName("movie-recommender-ds");
        dataSource.setServerName("localhost");
        dataSource.setDatabaseName("movie_recommender");
        dataSource.setUser(username);
        dataSource.setPassword(password);
        dataSource.setMaxConnections(10);

        PostgreSQLJDBCDataModel dataModel = new PostgreSQLJDBCDataModel(dataSource,
                                                                        "movie_ratings",
                                                                        "user_id",
                                                                        "movie_id",
                                                                        "rating",
                                                                        "timestamp");
        MoviesBean recommendedMovies = new MoviesBean();

        try {
            logger.info("Finding similar items for movie id " + movieId);
            //ItemSimilarity itemSimilarity = new PearsonCorrelationSimilarity(dataModel);
            ItemSimilarity itemSimilarity = new LogLikelihoodSimilarity(dataModel);
            ItemBasedRecommender recommender = new GenericItemBasedRecommender(dataModel, itemSimilarity);

            List<RecommendedItem> recommendations = recommender.mostSimilarItems(movieId, 5);
            for (RecommendedItem item : recommendations) {
                MovieBean m = new MovieBean();
                m.setId((int) item.getItemID());
                m.setRating(item.getValue());
                recommendedMovies.addMovieBean(m);
            }
            logger.info("Similar items are: " + recommendations);
            logger.info("MoviesBean is items are: " + recommendedMovies);



        } catch (TasteException e) {
            logger.error("There was a taste error: " + e);
        }

        return recommendedMovies;
    }

}
