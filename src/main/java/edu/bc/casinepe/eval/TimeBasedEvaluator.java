package edu.bc.casinepe.eval;

import com.google.common.collect.Lists;
import edu.bc.casinepe.jdbc.MysqlDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.*;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.common.RandomUtils;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by petercasinelli on 3/29/14.
 */
public class TimeBasedEvaluator {

    private static Logger logger = LogManager.getLogger(TimeBasedEvaluator.class);
    private RunningAverage runningAverage;

    public double[] evaluate(DataModel dataModel,
                             RecommenderBuilder recommenderBuilder,
                             long userId) {

        logger.info("Evaluating time based evaluator for user: " + userId);
        System.out.println("User ID: " + userId);
        System.out.println("item_id,aad,rmse,ratings_before");

        runningAverage = new FullRunningAverage();

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        // Get list of preferences for user userId
        try {

            logger.info("Getting all preferences for " + userId);

            // prefs contains all preferences for user userId ordered by timestamp
            PreferenceArray prefs = null;

            conn = MysqlDataSource.getMysqlDataSource().getConnection();
            stmt = conn.prepareStatement("SELECT movie_id, timestamp, rating FROM movie_ratings WHERE user_id=? ORDER BY timestamp ASC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setLong(1, userId);

            rs = stmt.executeQuery();

            List<Preference> userPrefs = Lists.newArrayList();
            while (rs.next()) {
                userPrefs.add(new GenericPreference(userId, rs.getLong("movie_id"), rs.getFloat("rating")));
            }

            if (userPrefs.isEmpty()) {
                throw new NoSuchUserException(userId);
            }

            logger.info("Retrieved all ratings for user " + userId + " which has a total of " + userPrefs.size() + " ratings");

            prefs = new GenericUserPreferenceArray(userPrefs);

            rs.close();
            stmt.close();
            conn.close();


            int numberOfRatingsForUser = 0;

            // For all of userId's preferences, find all preferences that occurred before it
            for (Preference currentPref : prefs) {

                RunningAverage aadAverage = new FullRunningAverage();
                RunningAverage rmseAverage = new FullRunningAverage();


                long prefTimestamp = dataModel.getPreferenceTime(userId, currentPref.getItemID());

                logger.info("Looking at all preferences before preference " + currentPref.getItemID());
                logger.info("User: " + userId + " Preference: " + currentPref.getItemID() + " At time " + prefTimestamp);


                // Find all ratings less than or equal to original preference's timestamp.
                // Keep track of all preferences that occurred before currentPref in this FastByIDMap
                FastByIDMap<PreferenceArray> preferencesBeforeCurrentPref = new FastByIDMap<PreferenceArray>();

                LongPrimitiveIterator it = dataModel.getUserIDs();
                int otherPreferenceCount = 0;

                conn = MysqlDataSource.getMysqlDataSource().getConnection();
                stmt = conn.prepareStatement("SELECT movie_id, user_id, rating FROM movie_ratings WHERE timestamp <= ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                stmt.setLong(1, prefTimestamp);

                rs = stmt.executeQuery();

                FastMap<Long, List<Preference>> preferencesFromDb = new FastMap<Long, List<Preference>>();

                // For every preference that occurred before currentPref
                while (rs.next()) {
                    long dbUserId = rs.getLong("user_id");
                    long dbMovieId = rs.getLong("movie_id");
                    long dbRating = rs.getLong("rating");

                    /*In the event that there are ratings with same timestamp, only ignore
                      current preference
                    */
                    if (dbMovieId == currentPref.getItemID()) {
                        continue;
                    }

                    List<Preference> previousPreferences = preferencesFromDb.get(dbUserId); //= preferencesFromDb.get(dbUserId);
                    if (previousPreferences == null) {
                        previousPreferences = new LinkedList<Preference>();
                        preferencesFromDb.put(dbUserId, previousPreferences);
                    }

                    previousPreferences.add(new GenericPreference(dbUserId, dbMovieId, dbRating));
                    otherPreferenceCount++;

                }

                logger.info("There have been " + otherPreferenceCount + " that occurred before item id " + currentPref.getItemID());
                //System.out.println("There are " + otherPreferenceCount + " ratings before item " + currentPref.getItemID());
                // Build FastByIdMap for DataModel
                for (Map.Entry<Long, List<Preference>> entry : preferencesFromDb.entrySet()) {
                    preferencesBeforeCurrentPref.put(entry.getKey(), new GenericUserPreferenceArray(entry.getValue()));
                }

                // Add NO preferences for this userId if there were no preferences before timestamp
                if (preferencesBeforeCurrentPref.get(userId) == null) {
                    logger.info("User " + userId + " had no ratings before preference " + currentPref.getItemID());
                    preferencesBeforeCurrentPref.put(userId, new GenericUserPreferenceArray(1));
                } else {
                    logger.info("User " + userId + " had " + preferencesBeforeCurrentPref.get(userId).length() + " ratings before preference " + currentPref.getItemID());
                }

                logger.info("There were " + otherPreferenceCount + " that occurred before item: " + currentPref.getItemID());
                //System.out.println("There were " + otherPreferenceCount + " that occurred before item: " + currentPref.getItemID());


                // Use these preferences to predict all of this user's ratings. Add them to a data model
                DataModel trainingModel = new GenericDataModel(preferencesBeforeCurrentPref);

                // Build a recommender using the training model
                Recommender recommender = recommenderBuilder.buildRecommender(trainingModel);

                for (Preference everyPref : prefs) {

                    if (everyPref.getItemID() == currentPref.getItemID()) {
                        continue;
                    }

                    float estimatedPreference = recommender.estimatePreference(userId, everyPref.getItemID());
                    //If estimated preference is not a NaN,
                    if (!Double.isNaN(estimatedPreference)) {
                        float actualPreference = dataModel.getPreferenceValue(userId, everyPref.getItemID());
                        double diff = actualPreference - estimatedPreference;

                        logger.info("Estimated preference for user " + userId + " and preference " + everyPref.getItemID() + " was " + estimatedPreference + " (predicted) \n" +
                                " Actual preference was " + actualPreference + " and diff is " + diff);


                        aadAverage.addDatum(Math.abs(diff));
                        rmseAverage.addDatum(diff * diff);

                    } else {
                        logger.info("Estimated preference for user " + userId + " and preference " + everyPref.getItemID() + " was NaN and could not be predicted");
                    }

                }

                //Update overall Average Absolute Difference running average
                runningAverage.addDatum(aadAverage.getAverage());
                System.out.println(currentPref.getItemID() + "," + aadAverage.getAverage() + "," + rmseAverage.getAverage() + "," + otherPreferenceCount);
                logger.info(currentPref.getItemID() + "," + aadAverage.getAverage() + "," + rmseAverage.getAverage() + "," + otherPreferenceCount);

                /*System.out.println("Average Absolute Difference After " + (++numberOfRatingsForUser) + " ratings for user: " + userId + " is: " + aadAverage.getAverage());
                System.out.println("RMSE After " + numberOfRatingsForUser + " ratings for user: " + userId + " is: " + rmseAverage.getAverage());

                logger.info("Results for preference: " + currentPref.getItemID() + " after " + numberOfRatingsForUser + "\n" +
                        "Average Absolute Difference: " + aadAverage.getAverage() + "\n" +
                        "RMSE: " + rmseAverage.getAverage());                                                                                    */
            }

        } catch (TasteException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }


        return new double[]{runningAverage.getAverage()};
    }


    public double[] evaluateNewPreferences(DataModel dataModel,
                             RecommenderBuilder recommenderBuilder,
                             long userId,
                             double evaluationPercentage) {

        logger.info("Evaluating preference data for user: " + userId);

        final Random random = RandomUtils.getRandom();

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        // Keep average metrics for this pref
        RunningAverage totalAadAverage = new FullRunningAverage();
        RunningAverage totalRmseAverage = new FullRunningAverage();

        // Get list of preferences for user userId
        try {

            logger.info("Getting all preferences for " + userId);

            // prefs contains all preferences for userId
            PreferenceArray prefs = null;

            conn = MysqlDataSource.getMysqlDataSource().getConnection();
            stmt = conn.prepareStatement("SELECT movie_id, timestamp, rating FROM movie_ratings WHERE user_id=? ORDER BY timestamp ASC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setLong(1, userId);

            rs = stmt.executeQuery();

            List<Preference> userPrefs = Lists.newArrayList();
            while (rs.next()) {
                userPrefs.add(new GenericPreference(userId, rs.getLong("movie_id"), rs.getFloat("rating")));
            }

            if (userPrefs.isEmpty()) {
                throw new NoSuchUserException(userId);
            }

            logger.info("Retrieved all ratings for user " + userId + " which has a total of " + userPrefs.size() + " ratings");

            prefs = new GenericUserPreferenceArray(userPrefs);
            FastByIDMap<PreferenceArray> currentUsersPreferences = new FastByIDMap<PreferenceArray>();
            currentUsersPreferences.put(userId, prefs);
            DataModel currentUsersDataModel = new GenericDataModel(currentUsersPreferences);


            rs.close();
            stmt.close();
            conn.close();

            // Get all preferences in DataModel BESIDES preferences from userId
            FastByIDMap<PreferenceArray> allOtherPreferences = new FastByIDMap<PreferenceArray>();
            LongPrimitiveIterator allUsers = dataModel.getUserIDs();
            while (allUsers.hasNext()) {
                long nextUserId = allUsers.nextLong();

                // do not add this userId's preferences; should happen once
                if (nextUserId == userId) {
                    continue;
                }

                if (random.nextDouble() >= evaluationPercentage) {
                    // Skipped
                    continue;
                }

                PreferenceArray nextUsersPreferences = dataModel.getPreferencesFromUser(nextUserId);
                allOtherPreferences.put(nextUserId, nextUsersPreferences);

            }

            // Now allOtherPreferencesModel contains all preferences besides userId's preferences
            DataModel allOtherPreferencesDataModel = new GenericDataModel(allOtherPreferences);

            PreferenceArray incrementalUserPreferences = new GenericUserPreferenceArray(userPrefs.size());
            // Introduce one preference at a time for userId into allOtherPreferencesModel
            for (int i = 0; i < prefs.length(); i++) {

                RunningAverage aadAverage = new FullRunningAverage();
                RunningAverage rmseAverage = new FullRunningAverage();

                //Get next preference
                Preference pref = prefs.get(i);

                /*System.out.println("Introducing a rating from userId: "
                        + pref.getUserID() + " Item: "
                        + pref.getItemID() + " Val: " + pref.getValue());*/

                // Introduce the preference to data model
                incrementalUserPreferences.set(i, new GenericPreference(pref.getUserID(), pref.getItemID(), pref.getValue()));
                allOtherPreferences.put(pref.getUserID(), incrementalUserPreferences);

                // Now estimate all other ratings from prefs
                for (int j = i+1; j < prefs.length() - 1; j++) {

                    Preference otherPref = prefs.get(j);

                    if (otherPref.getUserID() != userId) {
                        System.out.println("Other Pref UserId: " + otherPref.getUserID() + " UserId: " + userId + " Pref UserId: " + pref.getUserID());
                        System.exit(1);
                    }

                    Recommender recommender = recommenderBuilder.buildRecommender(allOtherPreferencesDataModel);

                    float estimatedPreference = recommender.estimatePreference(otherPref.getUserID(), otherPref.getItemID());

                    //If estimated preference is not a NaN,
                    if (!Double.isNaN(estimatedPreference)) {
                        //Get actual preference from the current userId's DataModel
                        float actualPreference = currentUsersDataModel.getPreferenceValue(otherPref.getUserID(), otherPref.getItemID());
                        double diff = actualPreference - estimatedPreference;

                        logger.info("Estimated preference for user " + otherPref.getUserID() + " and preference " + otherPref.getItemID() + " was " + estimatedPreference + " (predicted) \n" +
                                " Actual preference was " + actualPreference + " and diff is " + diff);

                        aadAverage.addDatum(Math.abs(diff));
                        totalAadAverage.addDatum(Math.abs(diff));

                        rmseAverage.addDatum(diff * diff);
                        totalRmseAverage.addDatum(diff * diff);


                    } else {
                        logger.info("Estimated preference for user " + userId + " and preference " + otherPref.getItemID() + " was NaN and could not be predicted");
                    }

                }

                int prefCount = i + 1;

                System.out.println(pref.getUserID() + "," + pref.getItemID() + "," + aadAverage.getAverage() + "," + rmseAverage.getAverage() + "," + prefCount);

            }

        System.out.println("Total AAD Average: " + totalAadAverage.getAverage());
        System.out.println("Total RMSE Average: " + totalRmseAverage.getAverage());

        } catch (TasteException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }

        return new double[]{totalAadAverage.getAverage(), totalRmseAverage.getAverage()};
    }


}
