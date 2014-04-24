package edu.bc.casinepe.eval;

import com.codahale.metrics.Timer;
import edu.bc.casinepe.core.ConfidenceItemAverageRecommender;
import edu.bc.casinepe.core.ConfidenceItemUserAverageRecommender;
import edu.bc.casinepe.jdbc.MysqlDataSource;
import edu.bc.casinepe.metrics.MetricSystem;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.common.Weighting;
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
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.sql.DataSource;

import static com.codahale.metrics.MetricRegistry.name;

public class EvaluateRecommenders {
    private static Logger logger = LoggerFactory.getLogger(EvaluateRecommenders.class.getName());
    private final Timer modifiedItemAverageTimer = MetricSystem.metrics.timer(name(PreferenceIntroductionEvaluator.class, "modifiedItemAverage"));

    private double trainingPercentage;
    private double evaluationPercentage;
    private int increments;
    private int maxPreferences;
    public static double pessimisticValue;


    public EvaluateRecommenders(String[] args) {

        //Start the metric system
        MetricSystem.start();

        //Retrieve training and evaluation percentage from arguments
        this.trainingPercentage = Double.parseDouble(args[0]);
        this.evaluationPercentage = Double.parseDouble(args[1]);
        this.pessimisticValue = Double.parseDouble(args[2]);
        this.increments = Integer.parseInt(args[3]);
        this.maxPreferences = Integer.parseInt(args[4]);

        System.out.println("Training percentage is : " + trainingPercentage + " and evaluation percentage is: " + evaluationPercentage);
        System.out.println("Pessimistic value is: " + pessimisticValue);
        System.out.println("Increments value is: " + increments);
        System.out.println("Max Preferences: " + maxPreferences);




        try {

            //Reload from model does not include preferences when exporting from AbstractJDBCDataModel
            DataModel dataModel = new ReloadFromJDBCDataModel(new MySQLJDBCDataModel(MysqlDataSource.getDataSource(),
                                                                                    "movie_ratings",
                                                                                    "user_id",
                                                                                    "movie_id",
                                                                                    "rating",
                                                                                    "timestamp"));

            /*DataModel dataModel = new MySQLJDBCDataModel(MysqlDataSource.getDataSource(),
                    "movie_ratings",
                    "user_id",
                    "movie_id",
                    "rating",
                    "timestamp");*/

            /*PearsonCorrelationSimilarity pearsonCorrelationSimilarity = new PearsonCorrelationSimilarity(dataModel);
            LogLikelihoodSimilarity logLikelihoodSimilarity = new LogLikelihoodSimilarity(dataModel);*/

            //Start evaluation
            /*evaluateItemItemCF("pearson", dataModel);
            evaluateItemItemCF("loglikelihood", dataModel);*/

            /*evaluateUserUserCF("pearson", dataModel);
            evaluateUserUserCF("loglikelihood", dataModel);*/

            // Non-Personalized Recommenders

            /*evaluateItemAverageRecommender(dataModel);
            evaluateModifiedItemAverageRecommender(dataModel);*/

            /*evaluateItemUserAverageRecommender(dataModel);
            evaluateModifiedItemUserAverageRecommender(dataModel); */


            //evaluateUserCFNonTargetDatasetIncrements(dataModel, "pearson");
            //evaluateUserCFNonTargetDatasetIncrements(dataModel, "loglikelihood");

            //evaluateItemCFNonTargetDatasetIncrements(dataModel, "pearson");
           // evaluateItemCFNonTargetDatasetIncrements(dataModel, "loglikelihood");


           //TODO
           /*evaluateUserCFIntroduceNewPreferences(dataModel, "pearson");
           evaluateUserCFIntroduceNewPreferences(dataModel, "loglikelihood"); */

           /*evaluateItemCFIntroduceNewPreferences(dataModel, "pearson");
           evaluateItemCFIntroduceNewPreferences(dataModel, "loglikelihood");*/

           /*neighborhoodExperiment("pearson", dataModel, increments, maxPreferences);
           neighborhoodExperiment("loglikelihood", dataModel, increments, maxPreferences);*/

        } catch (TasteException e) {
            e.printStackTrace();
        }

    }


    public void neighborhoodExperiment(String similarityStrategy, DataModel dataModel, int incrementPreferencesBy, int maxPreferencesToUse) {

        PreferenceIntroductionEvaluator preferenceIntroductionEvaluator = new PreferenceIntroductionEvaluator();

        preferenceIntroductionEvaluator.averageRatingsByNeighbors(25,
                                                                  similarityStrategy,
                                                                  dataModel,
                                                                  incrementPreferencesBy,
                                                                  maxPreferencesToUse,
                                                                  trainingPercentage);
    }

    public RecommenderBuilder getUserCfRsBuilder(final int neighborhoodSize, final String similarityStrategy) {
        RecommenderBuilder builder = new RecommenderBuilder() {

            public Recommender buildRecommender(DataModel model) {
                // build and return the Recommender to evaluate here
                Recommender recommender = null;
                UserSimilarity us = null;
                try {

                    if (similarityStrategy.equals("pearson")) {
                        us = new PearsonCorrelationSimilarity(model, Weighting.WEIGHTED);
                    } else if (similarityStrategy.equals("loglikelihood")) {
                        us = new LogLikelihoodSimilarity(model);
                    } else {
                        throw new Exception("You must choose either Pearson Correlation or Log Likelihood");
                    }

                    UserNeighborhood neighborhood = new NearestNUserNeighborhood(neighborhoodSize, us, model);
                    recommender = new GenericUserBasedRecommender(model, neighborhood, us);
                } catch (TasteException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return recommender;
            }

        };

        return builder;
    }

    public RecommenderBuilder getItemCfRsBuilder(final String similarityStrategy) {
        RecommenderBuilder builder = new RecommenderBuilder() {

            public Recommender buildRecommender(DataModel model) {
                // build and return the Recommender to evaluate here
                Recommender recommender = null;
                ItemSimilarity is = null;

                try {

                    if (similarityStrategy.equals("pearson")) {
                        is = new PearsonCorrelationSimilarity(model, Weighting.WEIGHTED);
                    } else if (similarityStrategy.equals("loglikelihood")) {
                        is = new LogLikelihoodSimilarity(model);
                    } else {
                        throw new Exception("You must choose either Pearson Correlation or Log Likelihood");
                    }

                    recommender = new GenericItemBasedRecommender(model, is);
                } catch (TasteException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return recommender;
            }

        };

        return builder;
    }



    public void evaluateUserCFNonTargetDatasetIncrements(DataModel dataModel, String similarityStrategy) {
        logger.info("Evaluating User-User CF Introducing Non-Target Data in Increments with " + similarityStrategy.toString());

        PreferenceIntroductionEvaluator timeBasedEvaluator = new PreferenceIntroductionEvaluator();
        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        timeBasedEvaluator.introduceNonTargetDataIncrements(dataModel,
                                                            getUserCfRsBuilder(25,similarityStrategy),
                                                            similarityStrategy + "-user-user",
                                                            100000,
                                                            500000,
                                                            trainingPercentage,
                                                            evaluationPercentage);

    }

    public void evaluateItemCFNonTargetDatasetIncrements(DataModel dataModel, String similarityStrategy) {
        logger.info("Evaluating Item-Item CF Introducing Non-Target Data in Increments with " + similarityStrategy);

        PreferenceIntroductionEvaluator timeBasedEvaluator = new PreferenceIntroductionEvaluator();
        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        timeBasedEvaluator.introduceNonTargetDataIncrements(dataModel,
                                                            getItemCfRsBuilder(similarityStrategy),
                                                            similarityStrategy + "-item-item",
                                                            100000,
                                                            500000,
                                                            trainingPercentage,
                                                            evaluationPercentage);

    }

    public void evaluateUserCFIntroduceNewPreferences(DataModel dataModel, String similarityStrategy) {

        logger.info("Evaluating User-User CF introducing new preferences using " + similarityStrategy);

        PreferenceIntroductionEvaluator timeBasedEvaluator = new PreferenceIntroductionEvaluator();
        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        timeBasedEvaluator.introduceNewRatings(dataModel,
                getUserCfRsBuilder(25, similarityStrategy),
                similarityStrategy + "-user-user",
                increments,
                maxPreferences,
                trainingPercentage,
                evaluationPercentage);

    }

    public void evaluateItemCFIntroduceNewPreferences(DataModel dataModel, String similarityStrategy) {
        logger.info("Evaluating Item-Item CF introducing new preferences using " + similarityStrategy);

        PreferenceIntroductionEvaluator timeBasedEvaluator = new PreferenceIntroductionEvaluator();
        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        timeBasedEvaluator.introduceNewRatings(dataModel,
                getItemCfRsBuilder(similarityStrategy),
                similarityStrategy + "-item-item",
                increments,
                maxPreferences,
                trainingPercentage,
                evaluationPercentage);

    }

    private void evaluateItemAverageRecommender(DataModel dataModel) {
        System.out.println("Item Average Recommender");
        logger.info("Item Average Recommender");

        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        RecommenderBuilder builder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model) {
                // build and return the Recommender to evaluate here
                Recommender recommender = null;
                try {
                    recommender = new ItemAverageRecommender(model);
                } catch (TasteException e) {
                    e.printStackTrace();
                }
                return recommender;
            }
        };

        //calculateAverageAbsoluteDifference(builder, dataModel);
        //calculateRmse(builder, dataModel);
        calculateRecallPrecision(builder, dataModel);
    }

    private void evaluateModifiedItemAverageRecommender(final DataModel dataModel) {
        System.out.println("Modified Item Average Recommender");
        logger.info("Modified Item Average Recommender");

        final Timer.Context context = modifiedItemAverageTimer.time();
        try {
            //Ensures random testing results every test
            org.apache.mahout.common.RandomUtils.useTestSeed();

            RecommenderBuilder builder = new RecommenderBuilder() {
                public Recommender buildRecommender(DataModel model) {
                    // build and return the Recommender to evaluate here
                    Recommender recommender = null;
                    try {
                        recommender = new ConfidenceItemAverageRecommender(model);
                    } catch (TasteException e) {
                        e.printStackTrace();
                    }
                    return recommender;
                }
            };


            //calculateAverageAbsoluteDifference(builder, dataModel);
            //calculateRmse(builder, dataModel);
            calculateRecallPrecision(builder, dataModel, null);

        } finally {
            context.stop();
        }

    }

    private void evaluateItemUserAverageRecommender(DataModel dataModel) {
        System.out.println("Item User Average Recommender");
        logger.info("Item User Average Recommender");

        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        RecommenderBuilder builder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model) {
                // build and return the Recommender to evaluate here
                Recommender recommender = null;
                try {
                    recommender = new ItemUserAverageRecommender(model);
                } catch (TasteException e) {
                    e.printStackTrace();
                }
                return recommender;
            }
        };

        //calculateAverageAbsoluteDifference(builder, dataModel);
        //calculateRmse(builder, dataModel);
        calculateRecallPrecision(builder, dataModel);
    }

    private void evaluateModifiedItemUserAverageRecommender(final DataModel dataModel) {
        System.out.println("Modified Item User Average Recommender");
        logger.info("Modified Item User Average Recommender");

        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        RecommenderBuilder builder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model) {
                // build and return the Recommender to evaluate here
                Recommender recommender = null;
                try {
                    recommender = new ConfidenceItemUserAverageRecommender(model);
                } catch (TasteException e) {
                    e.printStackTrace();
                }
                return recommender;
            }

        };

        //calculateAverageAbsoluteDifference(builder, dataModel);
        //calculateRmse(builder, dataModel);
        calculateRecallPrecision(builder, dataModel, null);
        //calculateRecallPrecision(builder, dataModel);
    }

    public void evaluateItemItemCF(String similarityStrategy, DataModel dataModel) {

        System.out.println("Item-Item CF with " + similarityStrategy + "\n");
        logger.info("Item-Item CF with " + similarityStrategy);

        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        RecommenderBuilder builder = getItemCfRsBuilder(similarityStrategy);

        calculateAverageAbsoluteDifference(builder, dataModel);
        calculateRmse(builder, dataModel);
        //calculateRecallPrecision(builder, dataModel);

    }

    public void evaluateUserUserCF(String similarityStrategy, DataModel dataModel) {

        System.out.println("User-User CF with " + similarityStrategy + "\n");
        logger.info("User-User CF with " + similarityStrategy);

        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        RecommenderBuilder builder = getUserCfRsBuilder(25, similarityStrategy);

        calculateAverageAbsoluteDifference(builder, dataModel);
        calculateRmse(builder, dataModel);
        //calculateRecallPrecision(builder, dataModel);

    }



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
        this.calculateRecallPrecision(builder, dataModel, null);
    }

    public void calculateRecallPrecision(RecommenderBuilder builder, DataModel dataModel, IDRescorer idRescorer) {
        logger.info("Evaluation percentage is " + this.evaluationPercentage);
        RecommenderIRStatsEvaluator irStatsEvaluator = new GenericRecommenderIRStatsEvaluator();
        IRStatistics stats = null;
        //Evalute Precision and Recall
        try {
            stats = irStatsEvaluator.evaluate(builder, null, dataModel, idRescorer, 10, /*GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD*/ 0.25, this.evaluationPercentage);
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
