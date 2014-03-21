package edu.bc.casinepe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.ReloadFromJDBCDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import javax.sql.DataSource;



/**
 * Created by petercasinelli on 3/12/14.
 */
public class EvaluateRecommenders {
    private static Logger logger = LogManager.getLogger(EvaluateRecommenders.class.getName());

    private String dataSet;
    //private static final double TRAINING_PERCENTAGE = 0.8;
    //private static final double EVALUATION_PERCENTAGE = 0.2;
    private double trainingPercentage;
    private double evaluationPercentage;

    public EvaluateRecommenders(String[] args) {
        //Retrieve training and evaluation percentage from arguments
        this.trainingPercentage = Double.parseDouble(args[0]);
        this.evaluationPercentage = Double.parseDouble(args[1]);

        System.out.println("Training percentage is : " + trainingPercentage + " and evaluation percentage is: " + evaluationPercentage);

        //Start evaluation
        evaluateItemItemCF();
        evaluateUserUser("pearson");
        evaluateUserUser("loglike");
    }

    public void evaluateItemItemCF() {
        System.out.println("Item-Item Collaborative Filtering Pearson Correlation: \n");
        logger.info("Item-Item CF Pearson Correlation");
        org.apache.mahout.common.RandomUtils.useTestSeed();
        DataSource dataSource = MysqlDataSource.getMysqlDataSource();

        DataModel dataModel;
        try {

            dataModel = new ReloadFromJDBCDataModel(new MySQLJDBCDataModel(dataSource,
                    "movie_ratings",
                    "user_id",
                    "movie_id",
                    "rating",
                    "timestamp"));

            RecommenderBuilder builder = new RecommenderBuilder() {
                public Recommender buildRecommender(DataModel model) {
                    // build and return the Recommender to evaluate here
                    Recommender recommender = null;
                    ItemSimilarity itemSimilarity = null;
                    try {
                        itemSimilarity = new PearsonCorrelationSimilarity(model);
                        recommender = new GenericItemBasedRecommender(model, itemSimilarity);
                    } catch (TasteException e) {
                        e.printStackTrace();
                    }


                    return recommender;
                }
            };

            RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
            RecommenderEvaluator rmseEvaluator = new RMSRecommenderEvaluator();
            double evaluation = 0;
            double rmseScore = 0;

            evaluation = evaluator.evaluate(builder, null, dataModel, this.trainingPercentage, this.evaluationPercentage);
            rmseScore = rmseEvaluator.evaluate(builder, null, dataModel, this.trainingPercentage, this.evaluationPercentage);

            System.out.println("Average Absolute Difference: " + evaluation);
            System.out.println("RMSE: " + rmseScore);

        } catch (TasteException e) {
            e.printStackTrace();
        }


    }

    public void evaluateUserUser(final String similarityStrategy) {

        System.out.println("User-User Collaborative Filtering " + similarityStrategy + ": \n");
        logger.info("User-User CF " + similarityStrategy);

        //Ensures random testing results every time
        org.apache.mahout.common.RandomUtils.useTestSeed();

        DataSource dataSource = MysqlDataSource.getMysqlDataSource();

        final DataModel dataModel;
        try {

            dataModel = new ReloadFromJDBCDataModel(new MySQLJDBCDataModel(dataSource,
                    "movie_ratings",
                    "user_id",
                    "movie_id",
                    "rating",
                    "timestamp"));


            RecommenderBuilder builder = new RecommenderBuilder() {
                public Recommender buildRecommender(DataModel model) {
                    // build and return the Recommender to evaluate here
                    Recommender recommender = null;

                    try {

                        UserSimilarity userSimilarity;

                        //Establish UserSimilarity based on passed parameter
                        if (similarityStrategy.equals("pearson")) {
                            userSimilarity = new PearsonCorrelationSimilarity(model);
                        } else if (similarityStrategy.equals("loglike")) {
                            userSimilarity = new LogLikelihoodSimilarity(model);
                        } else {
                            throw new Exception("No UserSimilarity strategy specified.");
                        }

                        UserNeighborhood neighborhood = new NearestNUserNeighborhood(25, userSimilarity, model);
                        recommender = new GenericUserBasedRecommender(model, neighborhood, userSimilarity);
                    } catch (TasteException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    return recommender;

                }
            };

            RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
            RecommenderEvaluator rmseEvaluator = new RMSRecommenderEvaluator();
            RecommenderIRStatsEvaluator irStatsEvaluator = new GenericRecommenderIRStatsEvaluator();
            IRStatistics stats = null;


            //Evaluate AAD
            double evaluation = 0;
            evaluation = evaluator.evaluate(builder, null, dataModel, this.trainingPercentage, this.evaluationPercentage);
            System.out.println("Average Absolute Difference: " + evaluation);

            //Evaluate RMSE
            double rmseScore = 0;
            rmseScore = rmseEvaluator.evaluate(builder, null, dataModel, this.trainingPercentage, this.evaluationPercentage);
            System.out.println("RMSE: " + rmseScore);

            //Evalute Precision and Recall
            stats = irStatsEvaluator.evaluate(builder, null, dataModel, null, 10, GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1);
            System.out.println("Precision: " + stats.getPrecision());
            System.out.println("Recall: " + stats.getRecall());

        } catch (Exception e) {
            logger.error("Error: " + e);
            e.printStackTrace();
        }


    }


    public static void main(String[] args) {
        EvaluateRecommenders er = new EvaluateRecommenders(args);
    }

}
