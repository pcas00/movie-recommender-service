package edu.bc.casinepe;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.jdbc.PostgreSQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.recommender.slopeone.SlopeOneRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.glassfish.grizzly.http.server.Response;
import org.postgresql.ds.PGPoolingDataSource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Created by petercasinelli on 3/12/14.
 */
@Path("evaluator")
@Produces(MediaType.APPLICATION_JSON)
public class EvaluatorResource {

    @GET
    public double evaluatePersonalizedRecommendations() {
        org.apache.mahout.common.RandomUtils.useTestSeed();
        PGPoolingDataSource dataSource = PGDataSource.getDataSource();
        DataModel dataModel = new PostgreSQLJDBCDataModel(dataSource,
                "movie_ratings",
                "user_id",
                "movie_id",
                "rating",
                "timestamp");

        RecommenderBuilder builder = new RecommenderBuilder() {
            public Recommender buildRecommender(DataModel model) {
                // build and return the Recommender to evaluate here
                Recommender recommender = null;
                try {
                    recommender = new SlopeOneRecommender(model);
                } catch (TasteException e) {
                    e.printStackTrace();
                }

                return recommender;
            }
        };
        RecommenderEvaluator evaluator = new AverageAbsoluteDifferenceRecommenderEvaluator();
        double evaluation = 0;
        try {
            evaluation = evaluator.evaluate(builder, null, dataModel, 0.9, 1.0);
        } catch (TasteException e) {
            e.printStackTrace();
        }

        return evaluation;
    }

}
