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

public class EvaluateRecommenders {
    private static Logger logger = LogManager.getLogger(EvaluateRecommenders.class.getName());
    private double trainingPercentage;
    private double evaluationPercentage;

    public EvaluateRecommenders(String[] args) {
        //Retrieve training and evaluation percentage from arguments
        this.trainingPercentage = Double.parseDouble(args[0]);
        this.evaluationPercentage = Double.parseDouble(args[1]);

        System.out.println("Training percentage is : " + trainingPercentage + " and evaluation percentage is: " + evaluationPercentage);

        DataSource dataSource = MysqlDataSource.getMysqlDataSource();
        try {
            DataModel dataModel = new ReloadFromJDBCDataModel(new MySQLJDBCDataModel(dataSource,
                    "movie_ratings",
                    "user_id",
                    "movie_id",
                    "rating",
                    "timestamp"));

            PearsonCorrelationSimilarity pearsonCorrelationSimilarity = new PearsonCorrelationSimilarity(dataModel);
            LogLikelihoodSimilarity logLikelihoodSimilarity = new LogLikelihoodSimilarity(dataModel);

            //Start evaluation
            /*evaluateItemItemCF(pearsonCorrelationSimilarity, dataModel);
            evaluateItemItemCF(logLikelihoodSimilarity, dataModel);*/

            evaluateUserUser(pearsonCorrelationSimilarity, dataModel);
            evaluateUserUser(logLikelihoodSimilarity, dataModel);

        } catch (TasteException e) {
            e.printStackTrace();
        }

    }

    public void evaluateItemItemCF(final ItemSimilarity similarityStrategy, DataModel dataModel) {

        System.out.println("Item-Item CF with " + similarityStrategy.getClass() + "\n");
        logger.info("Item-Item CF with " + similarityStrategy.getClass());

        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        RecommenderBuilder builder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model) {
                // build and return the Recommender to evaluate here
                Recommender recommender = new GenericItemBasedRecommender(model, similarityStrategy);
                return recommender;
            }
        };

        calculateAverageAbsoluteDifference(builder, dataModel);
        calculateRmse(builder, dataModel);
        //calculateRecallPrecision(builder, dataModel);

    }

    public void evaluateUserUser(final UserSimilarity similarityStrategy, DataModel dataModel) {

        System.out.println("User-User CF with " + similarityStrategy.getClass() + "\n");
        logger.info("User-User CF with " + similarityStrategy.getClass());

        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        RecommenderBuilder builder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model) {
                // build and return the Recommender to evaluate here
                Recommender recommender = null;

                try {
                    UserNeighborhood neighborhood = new NearestNUserNeighborhood(25, similarityStrategy, model);
                    recommender = new GenericUserBasedRecommender(model, neighborhood, similarityStrategy);

                } catch (TasteException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                return recommender;

            }
        };

        calculateAverageAbsoluteDifference(builder, dataModel);
        calculateRmse(builder, dataModel);
        //calculateRecallPrecision(builder, dataModel);

    }

    /*public void timeBackEvaluation(RecommenderBuilder builder, DataModel dataModel, long userId) {

        try {

            PreferenceArray userPreferences = dataModel.getPreferencesFromUser(userId);

            for (Preference currentPreference : userPreferences) {
                long currentPreferenceTimeStamp = dataModel.getPreferenceTime(userId, currentPreference.getItemID());

                for (Preference comparePreference : userPreferences) {
                    //Skip the same preference
                    if (comparePreference.getItemID() == currentPreference.getItemID()) {
                        continue;
                    }

                    //Skip preferences that occurred after current preferences' timestamp
                    if (dataModel.getPreferenceTime(userId, comparePreference.getItemID()) == currentPreferenceTimeStamp) {
                        continue;
                    }



                }

            }



        } catch (TasteException e) {
            e.printStackTrace();
        }

    } */

    public void calculateAverageAbsoluteDifference(RecommenderBuilder builder, DataModel dataModel) {
        String msg = "Calculating the average absolute difference...";
        logger.info(msg);
        System.out.println(msg);

        RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
        //Evaluate AAD
        double evaluation = Double.NaN;
        try {
            evaluation = evaluator.evaluate(builder, null, dataModel, this.trainingPercentage, this.evaluationPercentage);
        } catch (TasteException e) {
            e.printStackTrace();
        }
        logger.info("Average Absolute Difference: " + evaluation);
        System.out.println("Average Absolute Difference: " + evaluation);

    }

    public void calculateRmse(RecommenderBuilder builder, DataModel dataModel) {
        String msg = "Calculating the RMSE...";
        logger.info(msg);
        System.out.println(msg);

        RecommenderEvaluator rmseEvaluator = new RMSRecommenderEvaluator();
        //Evaluate RMSE
        double rmseScore = Double.NaN;
        try {
            rmseScore = rmseEvaluator.evaluate(builder, null, dataModel, this.trainingPercentage, this.evaluationPercentage);
        } catch (TasteException e) {
            e.printStackTrace();
        }
        logger.info("RMSE: " + rmseScore);
        System.out.println("RMSE: " + rmseScore);

    }

    public void calculateRecallPrecision(RecommenderBuilder builder, DataModel dataModel) {
        RecommenderIRStatsEvaluator irStatsEvaluator = new GenericRecommenderIRStatsEvaluator();
        IRStatistics stats = null;
        //Evalute Precision and Recall
        try {
            stats = irStatsEvaluator.evaluate(builder, null, dataModel, null, 10, /*GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD*/ 0.25, this.evaluationPercentage);
        } catch (TasteException e) {
            e.printStackTrace();
        }

        System.out.println("Precision: " + stats.getPrecision());
        System.out.println("Recall: " + stats.getRecall());
    }


    public static void main(String[] args) {
        EvaluateRecommenders er = new EvaluateRecommenders(args);
    }

}
