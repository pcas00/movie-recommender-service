package edu.bc.casinepe.eval;

import com.codahale.metrics.Timer;
import edu.bc.casinepe.jdbc.MysqlDataSource;
import edu.bc.casinepe.metrics.MetricSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
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

import javax.sql.DataSource;
import java.util.Random;

import static com.codahale.metrics.MetricRegistry.name;

public class EvaluateRecommenders {
    private static Logger logger = LogManager.getLogger(EvaluateRecommenders.class.getName());
    private final Timer modifiedItemAverageTimer = MetricSystem.metrics.timer(name(TimeBasedEvaluator.class, "modifiedItemAverage"));

    private double trainingPercentage;
    private double evaluationPercentage;
    private double pessimisticValue;

    public EvaluateRecommenders(String[] args) {

        //Start the metric system
        MetricSystem.start();

        //Retrieve training and evaluation percentage from arguments
        this.trainingPercentage = Double.parseDouble(args[0]);
        this.evaluationPercentage = Double.parseDouble(args[1]);
        this.pessimisticValue = Double.parseDouble(args[2]);

        System.out.println("Training percentage is : " + trainingPercentage + " and evaluation percentage is: " + evaluationPercentage);
        System.out.println("Pessimistic value is: " + pessimisticValue);


        try {
            //Reload from model does not include preferences when exporting from AbstractJDBCDataModel
            /*DataModel dataModel = new ReloadFromJDBCDataModel(new MySQLJDBCDataModel(dataSource,
                                                                                    "movie_ratings",
                                                                                    "user_id",
                                                                                    "movie_id",
                                                                                    "rating",
                                                                                    "timestamp"));*/

            DataModel dataModel = new MySQLJDBCDataModel(MysqlDataSource.getDataSource(),
                    "movie_ratings",
                    "user_id",
                    "movie_id",
                    "rating",
                    "timestamp");

            PearsonCorrelationSimilarity pearsonCorrelationSimilarity = new PearsonCorrelationSimilarity(dataModel);
            LogLikelihoodSimilarity logLikelihoodSimilarity = new LogLikelihoodSimilarity(dataModel);

            //Start evaluation
            /*evaluateItemItemCF(pearsonCorrelationSimilarity, dataModel);
            evaluateItemItemCF(logLikelihoodSimilarity, dataModel);

            evaluateUserUserCF(pearsonCorrelationSimilarity, dataModel);
            evaluateUserUserCF(logLikelihoodSimilarity, dataModel);*/

            // Non-Personalized Recommenders

            //evaluateItemAverageRecommender(dataModel);
            //evaluateModifiedItemAverageRecommender(dataModel);
            /*evaluateItemUserAverageRecommender(dataModel);
            evaluateModifiedItemUserAverageRecommender(dataModel);*/

            // Time Based Evaluation

            evaluateTimeBased(pearsonCorrelationSimilarity, dataModel);
            //evaluateIntroduceNewPreference(dataModel, pearsonCorrelationSimilarity);

        } catch (TasteException e) {
            e.printStackTrace();
        }

    }

    public void evaluateIntroduceNewPreference(DataModel dataModel, final ItemSimilarity similarityStrategy) {
        // System.out.println("user_id,item_id,aad,rmse,pref_count");
        logger.info("Evaluating introducing new preferences using " + similarityStrategy.getClass());
        TimeBasedEvaluator timeBasedEvaluator = new TimeBasedEvaluator();
        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        RecommenderBuilder builder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model) {
                // build and return the Recommender to evaluate here
                Recommender recommender = null;
                try {
                    recommender = new CachingRecommender(new GenericItemBasedRecommender(model, similarityStrategy));
                } catch (TasteException e) {
                    e.printStackTrace();
                }
                return recommender;
            }
        };

        timeBasedEvaluator.introduceNewRatings(dataModel, builder, 10, 76);


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
                    recommender = new CachingRecommender(new ItemAverageRecommender(model));
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
                        recommender = new CachingRecommender(new ItemAverageRecommender(model));
                    } catch (TasteException e) {
                        e.printStackTrace();
                    }
                    return recommender;
                }
            };

            IDRescorer customIdRescorer = new IDRescorer() {
                @Override
                public double rescore(long id, double originalScore) {
                    double newScore = originalScore;
                    try {
                        int numberOfRatings = dataModel.getNumUsersWithPreferenceFor(id);
                        newScore = originalScore - pessimisticValue / Math.sqrt(numberOfRatings);
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

            //calculateAverageAbsoluteDifference(builder, dataModel);
            //calculateRmse(builder, dataModel);
            calculateRecallPrecision(builder, dataModel, customIdRescorer);

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
                    recommender = new CachingRecommender(new ItemUserAverageRecommender(model));
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
                    recommender = new CachingRecommender(new ItemUserAverageRecommender(model));
                } catch (TasteException e) {
                    e.printStackTrace();
                }
                return recommender;
            }

        };

        IDRescorer customIdRescorer = new IDRescorer() {
            @Override
            public double rescore(long id, double originalScore) {
                double newScore = originalScore;
                try {
                    int numberOfRatings = dataModel.getNumUsersWithPreferenceFor(id);
                    newScore = originalScore - pessimisticValue / Math.sqrt(numberOfRatings);
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

        //calculateAverageAbsoluteDifference(builder, dataModel);
        //calculateRmse(builder, dataModel);
        calculateRecallPrecision(builder, dataModel, customIdRescorer);
        //calculateRecallPrecision(builder, dataModel);
    }

    private void evaluateTimeBased(final UserSimilarity similarityStrategy, DataModel dataModel) {

        logger.info("Calculating time based evaluation with similarity strategy: " + similarityStrategy.getClass());

        TimeBasedEvaluator timeBasedEvaluator = new TimeBasedEvaluator();

        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        RecommenderBuilder builder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model) {
                // build and return the Recommender to evaluate here
                Recommender recommender = null;

                try {
                    UserNeighborhood neighborhood = new NearestNUserNeighborhood(25, similarityStrategy, model);
                    recommender = new CachingRecommender(new GenericUserBasedRecommender(model, neighborhood, similarityStrategy));

                } catch (TasteException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                return recommender;

            }
        };

        timeBasedEvaluator.evaluateTimeContext(dataModel, builder, 10, 20);

    }

    public void evaluateItemItemCF(final ItemSimilarity similarityStrategy, DataModel dataModel) {

        System.out.println("Item-Item CF with " + similarityStrategy.getClass() + "\n");
        logger.info("Item-Item CF with " + similarityStrategy.getClass());

        //Ensures random testing results every test
        org.apache.mahout.common.RandomUtils.useTestSeed();

        RecommenderBuilder builder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model) {
                // build and return the Recommender to evaluate here
                Recommender recommender = null;
                try {
                    recommender = new CachingRecommender(new GenericItemBasedRecommender(model, similarityStrategy));
                } catch (TasteException e) {
                    e.printStackTrace();
                }
                return recommender;
            }
        };

        calculateAverageAbsoluteDifference(builder, dataModel);
        calculateRmse(builder, dataModel);
        //calculateRecallPrecision(builder, dataModel);

    }

    public void evaluateUserUserCF(final UserSimilarity similarityStrategy, DataModel dataModel) {

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
                    recommender = new CachingRecommender(new GenericUserBasedRecommender(model, neighborhood, similarityStrategy));

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
