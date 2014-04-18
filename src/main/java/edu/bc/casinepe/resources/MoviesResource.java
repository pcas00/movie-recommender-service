package edu.bc.casinepe.resources;

import com.codahale.metrics.Timer;
import edu.bc.casinepe.api.MovieBean;
import edu.bc.casinepe.api.MoviesBean;
import edu.bc.casinepe.core.FileRatings;
import edu.bc.casinepe.core.VectorOperations;
import edu.bc.casinepe.jdbc.JDBCDataModel;
import edu.bc.casinepe.jdbc.MovieDAO;
import edu.bc.casinepe.jdbc.MysqlDataSource;
import edu.bc.casinepe.metrics.MetricSystem;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.AbstractJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.ItemAverageRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Root resource (exposed at "movies" path)
 */
@Path("movies")
public class MoviesResource {

    private static Logger logger = LoggerFactory.getLogger(MoviesResource.class);
    private final Timer fileResponses = MetricSystem.metrics.timer(name(MoviesResource.class, "fileResponses"));
    private final Timer getMoviesFromDb = MetricSystem.metrics.timer(name(MoviesResource.class, "getMoviesFromDb"));
    private final Timer getSimilarMoviesFromDb = MetricSystem.metrics.timer(name(MoviesResource.class, "getSimilarMoviesFromDb"));


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getMovies() {
        logger.info("Get movies from file");
        final Timer.Context context = fileResponses.time();
        try {
            MoviesBean movies = new MoviesBean();
            //Map<Integer, List<Double>> movieRatings = FileRatings.parseMovieRatings("mahoutRatings.dat");
            File ratingsFile = new File(this.getClass().getResource("1mm-ratings.csv").getFile());
            logger.info("File is: " + ratingsFile);
            FileDataModel dataModel = new FileDataModel(ratingsFile);

            /*Map<Integer, List<Double>> top5Movies =  VectorOperations.topNFromMap(5, movieRatings);
            for (Map.Entry<Integer, List<Double>> entry : top5Movies.entrySet()) {
                MovieBean m = new MovieBean();
                List<Double> ratings = entry.getValue();
                m.setId(entry.getKey());
                m.setRating(VectorOperations.mean(ratings));
                m.setRatingsCount(ratings.size());
                movies.addMovieBean(m);
            }*/
            ItemAverageRecommender itemAverageRecommender = new ItemAverageRecommender(dataModel);
            List<RecommendedItem> recommendedItems = itemAverageRecommender.recommend(9, 5);
            MovieDAO movieDAO = new MovieDAO();
            for (RecommendedItem item : recommendedItems) {
                MovieBean m = movieDAO.getMovie((int)item.getItemID());
                movies.addMovieBean(m);
            }
            logger.info(movies.getMovies().size() + " movies have been recommended");

            return movies;

        } catch (IOException e) {
            logger.error("Error: " + e.getMessage());
            e.printStackTrace();
            //Should be an error message JSON response
            return new MoviesBean();
        } catch (TasteException e) {
            logger.error("Error: " + e.getMessage());
            e.printStackTrace();
            return new MoviesBean();
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

            ItemAverageRecommender itemAverageRecommender = new ItemAverageRecommender(JDBCDataModel.getDataModel());
            List<RecommendedItem> recommendedItems = itemAverageRecommender.recommend(9, 5);
            MovieDAO movieDAO = new MovieDAO();
            for (RecommendedItem item : recommendedItems) {
                MovieBean m = movieDAO.getMovie((int)item.getItemID());
                movies.addMovieBean(m);
            }
            logger.info(movies.getMovies().size() + " movies have been recommended");

            return movies;

        } catch (TasteException e) {
            logger.error("TasteException: {}", e);
        } finally {
            context.stop();
        }

        return null;
    }

    @GET
    @Path("/custom/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getMoviesFromAverageScorer(@PathParam("userId") int userId) {

        MoviesBean recommendedMovies = new MoviesBean();

        DataSource dataSource = MysqlDataSource.getDataSource();

        final AbstractJDBCDataModel dataModel = new MySQLJDBCDataModel(dataSource,
                "movie_ratings",
                "user_id",
                "movie_id",
                "rating",
                "timestamp");

        ItemAverageRecommender itemAverageRecommender = null;

        try {

            itemAverageRecommender = new ItemAverageRecommender(dataModel);
            IDRescorer idRescorer = new IDRescorer() {
                @Override
                public double rescore(long id, double originalScore) {
                    double newScore = originalScore;
                    try {
                        int numberOfRatings = dataModel.getNumUsersWithPreferenceFor(id);
                        newScore = originalScore - 2 / Math.sqrt(numberOfRatings);
                    } catch (TasteException e) {
                        e.printStackTrace();
                    } finally {
                        return newScore;
                    }
                }

                @Override
                public boolean isFiltered(long id) {
                    return false;
                }
            };

            List<RecommendedItem> recommendedItems = itemAverageRecommender.recommend(userId, 5, idRescorer);

            MovieDAO movieDAO = new MovieDAO();
            for (RecommendedItem item : recommendedItems) {
                MovieBean m = movieDAO.getMovie((int)item.getItemID());
                recommendedMovies.addMovieBean(m);
            }

        } catch (TasteException e) {
            e.printStackTrace();
        }

        return recommendedMovies;
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

            DataSource dataSource = MysqlDataSource.getDataSource();

            MySQLJDBCDataModel dataModel = new MySQLJDBCDataModel(dataSource,
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
            MovieDAO movieDAO = new MovieDAO();
            for (RecommendedItem item : recommendations) {
                MovieBean m = movieDAO.getMovie((int)item.getItemID());
                recommendedMovies.addMovieBean(m);
            }
            logger.info("Similar items are: " + recommendations);



        } catch (Exception e) {
            logger.error("There was a taste error: " + e);
        } finally {
            context.stop();

            return recommendedMovies;
        }

    }

    @GET
    @Path("/personalized/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getPersonalizedMovies(@PathParam("userId") int userId) {

        MoviesBean recommendedMovies = new MoviesBean();

        //DataSource dataSource = MysqlDataSource.getDataSource();

        final DataModel dataModel = JDBCDataModel.getDataModel();

        Recommender recommender = null;
        //Recommender cachingRecommender = null;

        try {
            ItemSimilarity itemSimilarity = new LogLikelihoodSimilarity(dataModel);
            recommender = new CachingRecommender(new GenericItemBasedRecommender(dataModel, itemSimilarity));
            List<RecommendedItem> recommendations = recommender.recommend(userId, 5);
            MovieDAO movieDAO = new MovieDAO();
            for (RecommendedItem item : recommendations) {
                MovieBean m = movieDAO.getMovie((int)item.getItemID());
                recommendedMovies.addMovieBean(m);
            }

        } catch (TasteException e) {

            logger.error(e.getMessage());

        } finally {

            return recommendedMovies;
        }
    }

}
