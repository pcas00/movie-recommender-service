package edu.bc.casinepe.core;

import edu.bc.casinepe.jdbc.JDBCDataModel;
import edu.bc.casinepe.jdbc.MysqlDataSource;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.neighborhood.CachingUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.*;
import org.apache.mahout.cf.taste.impl.similarity.GenericItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.CandidateItemsStrategy;
import org.apache.mahout.cf.taste.recommender.MostSimilarItemsCandidateItemsStrategy;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by petercasinelli on 4/24/14.
 */
public class RecommenderFactory {
    private static Logger logger = LoggerFactory.getLogger(RecommenderFactory.class);

    private static Recommender userUserPearsonRS = null;
    private static Recommender itemItemPearsonRS = null;
    private static Recommender userUserLogLikelihoodRS = null;
    private static Recommender itemItemLogLikelihoodRS = null;
    private static Recommender itemAverageRecommender = null;

    private static CachingUserNeighborhood pearsonNeighborhood = null;
    private static CachingUserNeighborhood loglikelihoodNeighborhood = null;

    private static MostSimilarItemsCandidateItemsStrategy mostSimilarItemsCandidateItemsStrategy = null;

    private static CandidateItemsStrategy candidateItemsStrategy = null;


    public static Recommender getItemAverageRecommender() {

        if (itemAverageRecommender == null) {
            try {

                itemAverageRecommender = new CachingRecommender(new ItemAverageRecommender(JDBCDataModel.getDataModel()));

            } catch (TasteException e) {
                logger.error("TasteException ", e);
            }

        }

        return itemAverageRecommender;
    }

    public static Recommender getUserUserPearsonCFRS() {
        try {

            if (userUserPearsonRS == null) {
                logger.info("UU Pearson not instantiated yet; creating now");
                if (pearsonNeighborhood == null) {
                    logger.info("Pearson neighborhood not instantiated yet; creating now");
                    pearsonNeighborhood = new CachingUserNeighborhood(new NearestNUserNeighborhood(25, SimilarityFactory.getPearsonUserSimilarity(), JDBCDataModel.getDataModel()), JDBCDataModel.getDataModel());
                }

                userUserPearsonRS = new CachingRecommender(new GenericUserBasedRecommender(JDBCDataModel.getDataModel(), pearsonNeighborhood, SimilarityFactory.getPearsonUserSimilarity()));

            }
        } catch (TasteException e) {
            logger.error("Error " , e);
            e.printStackTrace();
        }

        return userUserPearsonRS;
    }

    public static Recommender getUserUserLogCFRS() {
        try {
            if (userUserLogLikelihoodRS == null) {
                logger.info("UU Log Likelihood not instantiated yet; creating now");

                if (loglikelihoodNeighborhood == null) {
                    logger.info("Log likelihood neighborhood not instantiated yet; creating now");
                    loglikelihoodNeighborhood = new CachingUserNeighborhood(new NearestNUserNeighborhood(25, SimilarityFactory.getLoglikelihoodUserSimilarity(), JDBCDataModel.getDataModel()), JDBCDataModel.getDataModel());
                }

                userUserLogLikelihoodRS = new CachingRecommender(new GenericUserBasedRecommender(JDBCDataModel.getDataModel(),
                                                                                                 loglikelihoodNeighborhood,
                                                                                                 SimilarityFactory.getLoglikelihoodUserSimilarity()));

            }
        } catch (TasteException e) {
            logger.error("Error " , e);
            e.printStackTrace();
        }

        return userUserLogLikelihoodRS;
    }

    public static Recommender getItemItemPearsonCFRS() {

        try {

            if (itemItemPearsonRS == null) {
                logger.info("II Pearson not instantiated yet; creating now");

                /*if (candidateItemsStrategy == null) {
                    logger.info("Candidate items strategy null; creating now");
                    candidateItemsStrategy = new SamplingCandidateItemsStrategy(JDBCDataModel.getDataModel().getNumUsers(), JDBCDataModel.getDataModel().getNumItems());
                }

                if (mostSimilarItemsCandidateItemsStrategy == null) {
                    logger.info("Most Similar Items Candidate strategy null; creating now");
                    mostSimilarItemsCandidateItemsStrategy = new SamplingCandidateItemsStrategy(JDBCDataModel.getDataModel().getNumUsers(), JDBCDataModel.getDataModel().getNumItems());
                }

                itemItemPearsonRS = new CachingRecommender(new GenericItemBasedRecommender(JDBCDataModel.getDataModel(),
                                                                                           SimilarityFactory.getPearsonItemSimilarities(),
                                                                                           candidateItemsStrategy,
                                                                                           mostSimilarItemsCandidateItemsStrategy));*/

                itemItemPearsonRS = new CachingRecommender(new GenericItemBasedRecommender(JDBCDataModel.getDataModel(), /*SimilarityFactory.getPearsonItemSimilarity()*/SimilarityFactory.getPearsonItemSimilarities()));
                logger.info("Item Item Pearson Similarity RS loaded");
            }

        } catch (TasteException e) {
            logger.error("Error " , e);
            e.printStackTrace();
        }


        return itemItemPearsonRS;
    }

    public static Recommender getItemItemLogCFRS() {

        try {

            if (itemItemLogLikelihoodRS == null) {
                logger.info("II Log Likelihood not instantiated yet; creating now");

                /*if (candidateItemsStrategy == null) {
                    logger.info("Candidate items strategy null; creating now");
                    candidateItemsStrategy = new SamplingCandidateItemsStrategy(JDBCDataModel.getDataModel().getNumUsers(), JDBCDataModel.getDataModel().getNumItems());
                }

                if (mostSimilarItemsCandidateItemsStrategy == null) {
                    logger.info("Most Similar Items Candidate strategy null; creating now");
                    mostSimilarItemsCandidateItemsStrategy = new SamplingCandidateItemsStrategy(JDBCDataModel.getDataModel().getNumUsers(), JDBCDataModel.getDataModel().getNumItems());
                }

                itemItemLogLikelihoodRS = new CachingRecommender(new GenericItemBasedRecommender(JDBCDataModel.getDataModel(),
                                                                                                 SimilarityFactory.getLoglikelihoodItemSimilarities(),
                                                                                                 candidateItemsStrategy,
                                                                                                 mostSimilarItemsCandidateItemsStrategy));*/
                itemItemLogLikelihoodRS = new CachingRecommender(new GenericItemBasedRecommender(JDBCDataModel.getDataModel(), SimilarityFactory.getLoglikelihoodItemSimilarities()));
                logger.info("Item Item Log Likelihood Similarity RS loaded");

            }

        } catch (TasteException e) {
            logger.error("Error " , e);
            e.printStackTrace();
        }

        return itemItemLogLikelihoodRS;
    }

    public static Recommender getRecommenderSystem(String cf, String similarityStrategy) {

        Recommender rec = null;

        if (cf.equals("uu")) {

            if (similarityStrategy.equals("pearson")) {

                rec = getUserUserPearsonCFRS();

            } else if (similarityStrategy.equals("loglikelihood")) {

                rec = getUserUserLogCFRS();
            }

        } else if (cf.equals("ii")) {

            if (similarityStrategy.equals("pearson")) {

                rec = getItemItemPearsonCFRS();

            } else if (similarityStrategy.equals("loglikelihood")) {

                rec = getItemItemLogCFRS();

            }

        }

        return rec;


    }
}
