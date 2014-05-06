package edu.bc.casinepe.core;

import edu.bc.casinepe.jdbc.JDBCDataModel;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.similarity.*;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by petercasinelli on 4/24/14.
 */
public class SimilarityFactory {
    private static Logger logger = LoggerFactory.getLogger(SimilarityFactory.class);

    private static LogLikelihoodSimilarity logLikelihoodSimilarity = null;
    private static PearsonCorrelationSimilarity pearsonCorrelationSimilarity = null;
    private static UserSimilarity loglikelihoodUserSimilarity = null;
    private static ItemSimilarity loglikelihoodItemSimilarity = null;
    private static GenericItemSimilarity loglikelihoodItemSimilarities = null;
    private static UserSimilarity pearsonUserSimilarity = null;
    private static ItemSimilarity pearsonItemSimilarity = null;
    private static GenericItemSimilarity pearsonItemSimilarities = null;

    public static ItemSimilarity getPearsonItemSimilarity() throws TasteException {
        if (pearsonItemSimilarity == null) {
            logger.info("Getting Cached Pearson Item Similarity");
            pearsonItemSimilarity = new CachingItemSimilarity(getPearsonCorrelationSimilarity(), JDBCDataModel.getDataModel());
        }
        return pearsonItemSimilarity;
    }

    public static UserSimilarity getPearsonUserSimilarity() throws TasteException {
        if (pearsonUserSimilarity == null) {
            logger.info("Getting Cached Pearson User Similarity");
            pearsonUserSimilarity = new CachingUserSimilarity(getPearsonCorrelationSimilarity(), JDBCDataModel.getDataModel());
        }
        return pearsonUserSimilarity;
    }

    public static ItemSimilarity getLoglikelihoodItemSimilarity() throws TasteException {
        if (loglikelihoodItemSimilarity == null) {
            logger.info("Getting Cached Log Likelihood Item Similarity");
            loglikelihoodItemSimilarity = new CachingItemSimilarity(getLoglikelihoodSimilarity(), JDBCDataModel.getDataModel());
        }
        return loglikelihoodItemSimilarity;
    }

    public static UserSimilarity getLoglikelihoodUserSimilarity() throws TasteException {
        if (loglikelihoodUserSimilarity == null) {
            logger.info("Getting Cached Log Likelihood User Similarity");
            loglikelihoodUserSimilarity = new CachingUserSimilarity(getLoglikelihoodSimilarity(), JDBCDataModel.getDataModel());
        }
        return loglikelihoodUserSimilarity;
    }

    public static ItemSimilarity getPearsonItemSimilarities() throws TasteException {

        if (pearsonItemSimilarities == null) {
            logger.info("Getting Pearson Item Correlations");
            pearsonItemSimilarities = new GenericItemSimilarity(SimilarityFactory.getPearsonItemSimilarity(), JDBCDataModel.getDataModel());
        }
        return pearsonItemSimilarities;

    }

    public static ItemSimilarity getLoglikelihoodItemSimilarities() throws TasteException {

        if (loglikelihoodItemSimilarities == null) {
            logger.info("Getting Log likelihood Item Correlations");
            loglikelihoodItemSimilarities = new GenericItemSimilarity(SimilarityFactory.getLoglikelihoodSimilarity(), JDBCDataModel.getDataModel());
        }
        return loglikelihoodItemSimilarities;

    }


    private SimilarityFactory() {}

    private static LogLikelihoodSimilarity getLoglikelihoodSimilarity() {
        if (logLikelihoodSimilarity == null) {
            logger.info("Getting Log Likelihood Ratio similarity with datamodel");
            logLikelihoodSimilarity = new LogLikelihoodSimilarity(JDBCDataModel.getDataModel());
        }
        return logLikelihoodSimilarity;
    }

    private static PearsonCorrelationSimilarity getPearsonCorrelationSimilarity() {
        if (pearsonCorrelationSimilarity == null) {
            try {
                logger.info("Getting Pearson Correlation similarity with datamodel");
                pearsonCorrelationSimilarity = new PearsonCorrelationSimilarity(JDBCDataModel.getDataModel());
            } catch (TasteException e) {
                logger.error("TasteException: {}", e);
                e.printStackTrace();
            }
        }
        return pearsonCorrelationSimilarity;
    }


}
