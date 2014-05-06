package edu.bc.casinepe.resources;

import com.codahale.metrics.Timer;
import edu.bc.casinepe.api.MovieBean;
import edu.bc.casinepe.api.MoviesBean;
import edu.bc.casinepe.core.FileRatings;
import edu.bc.casinepe.core.RecommenderFactory;
import edu.bc.casinepe.jdbc.JDBCDataModel;
import edu.bc.casinepe.dao.MovieDAO;
import edu.bc.casinepe.jdbc.MysqlDataSource;
import edu.bc.casinepe.core.SimilarityFactory;
import edu.bc.casinepe.log.TimerFactory;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.AbstractJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.CachingUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.*;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.util.*;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Root resource (exposed at "movies" path)
 */
@Path("movies")
public class MoviesResource {

    private static Logger logger = LoggerFactory.getLogger(MoviesResource.class);
    /*private final Timer getRecFromFile = MetricSystem.metrics.timer(name(MoviesResource.class, "fileResponses"));
    private final Timer getMoviesFromDb = MetricSystem.metrics.timer(name(MoviesResource.class, "getMoviesFromDb"));
    private final Timer getSimilarMoviesFromDb = MetricSystem.metrics.timer(name(MoviesResource.class, "getSimilarMoviesFromDb"));
    */
    private Random random = new Random();


    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getMovies() {
        logger.info("Get movies from file");

        //final Timer.Context context = getRecFromFile.time();
        MoviesBean movies = new MoviesBean();
        logger.info("Before try");

        try {
            /*final FileDataModel dataModel = new FileDataModel(FileRatings.getRatingsFile());

            ItemAverageRecommender itemAverageRecommender = new ItemAverageRecommender(dataModel);*/

            IDRescorer customIdRescorer = new IDRescorer() {
                @Override
                public double rescore(long id, double originalScore) {
                    double newScore = originalScore;
                    try {
                        int numberOfRatings = JDBCDataModel.getDataModel().getNumUsersWithPreferenceFor(id);
                        newScore = originalScore - 20 / Math.sqrt(numberOfRatings);
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


            Recommender recommender = RecommenderFactory.getItemAverageRecommender();
            List<RecommendedItem> recommendedItems = recommender.recommend(9, 5, customIdRescorer);

            MovieDAO movieDAO = new MovieDAO();

            for (RecommendedItem item : recommendedItems) {
                MovieBean m = movieDAO.getMovieData((int)item.getItemID());
                m.setRating(item.getValue());
                movies.addMovieBean(m);
            }

            logger.info(movies.getMovies().size() + " movies have been recommended");

        }/* catch (IOException e) {
            logger.error("IOException Error: {}", e);
            e.printStackTrace();
        }*/ catch (TasteException e) {
            logger.error("TasteException Error: {}", e);
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("Exception {}", e);
        } finally {
            //context.stop();
        }

        return movies;

    }

    @GET
    @Path("/db/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getMoviesFromDB(@PathParam("userId") long userId) {

        //final Timer.Context context = getMoviesFromDb.time();

        try {
            // Create container for movies
            MoviesBean movies = new MoviesBean();

            final DataModel dataModel = JDBCDataModel.getDataModel();
            ItemAverageRecommender itemAverageRecommender = new ItemAverageRecommender(dataModel);

            IDRescorer customIdRescorer = new IDRescorer() {
                @Override
                public double rescore(long id, double originalScore) {
                    double newScore = originalScore;
                    try {
                        int numberOfRatings = dataModel.getNumUsersWithPreferenceFor(id);
                        newScore = originalScore - 20 / Math.sqrt(numberOfRatings);
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

            List<RecommendedItem> recommendedItems = itemAverageRecommender.recommend(userId, 5, customIdRescorer);
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
            //context.stop();
        }

        return null;
    }

    @GET
    @Path("/np/{userId}")
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
                        newScore = originalScore - 15 / Math.sqrt(numberOfRatings);
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
            //Only return a maximum of 10 similar items
        } else if (number > 10) {
            number = 10;
        }

        logger.info("Getting " + number + " similar movies to movie ID " + movieId);


        // Start timer
        //final Timer.Context context = getSimilarMoviesFromDb.time();
        MoviesBean recommendedMovies = new MoviesBean();

        try {

            DataModel dataModel = JDBCDataModel.getDataModel();

            logger.info("Finding similar items for movie id " + movieId);
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
            //context.stop();

            return recommendedMovies;
        }

    }

    private UserSimilarity getUserSimilarity(String similarityStrategy) {
        //Establish user similarity algorithm; can either be Pearson or Log Likelihood
        UserSimilarity userSimilarity = null;
        try {

            if (similarityStrategy.equals("pearson")) {
                logger.info("pearson similarity strategy");
                //userSimilarity = SimilarityFactory.getPearsonCorrelationSimilarity();
                userSimilarity = SimilarityFactory.getPearsonUserSimilarity();
            } else {
                logger.info("loglikelihood similarity strategy");
                userSimilarity = SimilarityFactory.getLoglikelihoodUserSimilarity();
            }

        } catch (TasteException e) {
            logger.error("TasteException ", e);
        } catch (Exception e) {
            logger.error("Exception ", e);
        }


        return userSimilarity;
    }

    private ItemSimilarity getItemSimilarity(String similarityStrategy) {
        //Establish user similarity algorithm; can either be Pearson or Log Likelihood
        ItemSimilarity itemSimilarity = null;
        try {

            if (similarityStrategy.equals("pearson")) {
                logger.info("pearson similarity strategy");
                itemSimilarity = SimilarityFactory.getPearsonItemSimilarity();
            } else if (similarityStrategy.equals("loglikelihood")) {
                logger.info("log likelihood similarity strategy");
                itemSimilarity = SimilarityFactory.getLoglikelihoodItemSimilarity();
            }

        } catch (TasteException e) {
            logger.error("TasteException ", e);
        } catch (Exception e) {
            logger.error("Exception ", e);
        }

        return itemSimilarity;
    }

    private DataModel getDataModel(String dataModelStrategy) {

        DataModel dataModel = null;
        if (dataModelStrategy.equals("file")) {
            logger.info("file data model");
            try {
                dataModel = new FileDataModel(FileRatings.getRatingsFile());
            } catch (IOException e) {
                logger.error("IOException ", e);
            }
        } else if (dataModelStrategy.equals("db")) {
            logger.info("db data model");
            dataModel = JDBCDataModel.getDataModel();
        }

        return dataModel;
    }



    @GET
    @Path("/uu/{dataModel}/similarity/{similarityStrategy}/user/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getUserUserCFRecommendations(@PathParam("dataModel") String dataModelStrategy,
                                                   @PathParam("similarityStrategy") String similarityStrategy,
                                                   @PathParam("userId") long userId) {

        //long userId = (long) (random.nextInt(6039) + 1);
        //long userId = 2;

        logger.info("UU CF with " + dataModelStrategy + " data model, " + similarityStrategy + " similarity algorithm, for user " + userId);
        MoviesBean recommendedMovies = new MoviesBean();

        final Timer.Context context = TimerFactory.getTimer(TimerFactory.getTimerType("uu", dataModelStrategy, similarityStrategy)).time();

        try {

            Recommender recommender = null;

            if (dataModelStrategy.equals("file")) {
                logger.info("File so do not use factory");
                final DataModel dataModel = getDataModel(dataModelStrategy);
                UserSimilarity userSimilarity = null;

                if (similarityStrategy.equals("pearson")) {
                    userSimilarity = new PearsonCorrelationSimilarity(dataModel);
                } else {
                    userSimilarity = new LogLikelihoodSimilarity(dataModel);
                }
                UserNeighborhood userNeighborhood = new NearestNUserNeighborhood(25, userSimilarity, dataModel);
                recommender = new GenericUserBasedRecommender(dataModel, userNeighborhood, userSimilarity);
            } else {
                logger.info("DB so using factory");
                recommender = RecommenderFactory.getRecommenderSystem("uu", similarityStrategy);
            }


            List<RecommendedItem> recommendations = recommender.recommend(userId, 5);
            MovieDAO movieDAO = new MovieDAO();
            for (RecommendedItem item : recommendations) {
                MovieBean m = movieDAO.getMovieData((int)item.getItemID());
                m.setRating(item.getValue());
                recommendedMovies.addMovieBean(m);
            }

        } catch (TasteException e) {
            logger.error(e.getMessage());
        } catch (Exception e) {
            logger.error("Exception ", e);
        } finally {
            context.stop();
            return recommendedMovies;
        }
    }

    @GET
    @Path("/ii/{dataModel}/similarity/{similarityStrategy}/user/{userId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getItemItemCFRecommendations(@PathParam("dataModel") String dataModelStrategy,
                                                   @PathParam("similarityStrategy") String similarityStrategy,
                                                   @PathParam("userId") long userId) {

        //long userId = (long) (random.nextInt(6039) + 1);
        //long userId = 2;


        logger.info("II CF with " + dataModelStrategy + " data model, " + similarityStrategy + " similarity algorithm, for user " + userId);
        MoviesBean recommendedMovies = new MoviesBean();

        final Timer.Context context = TimerFactory.getTimer(TimerFactory.getTimerType("ii", dataModelStrategy, similarityStrategy)).time();
        try {


            Recommender recommender = null;

            if (dataModelStrategy.equals("file")) {
                logger.info("File so do not use factory");
                final DataModel dataModel = getDataModel(dataModelStrategy);

                ItemSimilarity itemSimilarity = null;

                if (similarityStrategy.equals("pearson")) {
                    itemSimilarity = new PearsonCorrelationSimilarity(dataModel);
                } else {
                    itemSimilarity = new LogLikelihoodSimilarity(dataModel);
                }
                recommender = new GenericItemBasedRecommender(dataModel, itemSimilarity);
            } else {
                logger.info("DB so using factory");
                recommender = RecommenderFactory.getRecommenderSystem("ii", similarityStrategy);
            }

            List<RecommendedItem> recommendations = recommender.recommend(userId, 5);
            MovieDAO movieDAO = new MovieDAO();
            for (RecommendedItem item : recommendations) {
                MovieBean m = movieDAO.getMovieData((int)item.getItemID());
                m.setRating(item.getValue());
                recommendedMovies.addMovieBean(m);
            }

        } catch (TasteException e) {
            logger.error("TasteException ", e);
        } catch (Exception e) {
            logger.error("Exception ", e);
        } finally {
            context.stop();
            return recommendedMovies;
        }
    }


}
