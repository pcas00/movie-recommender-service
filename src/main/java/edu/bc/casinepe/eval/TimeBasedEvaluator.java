package edu.bc.casinepe.eval;

import com.codahale.metrics.Timer;
import com.google.common.collect.Lists;
import edu.bc.casinepe.jdbc.MysqlDataSource;
import edu.bc.casinepe.metrics.MetricSystem;
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.util.*;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by petercasinelli on 3/29/14.
 */
public class TimeBasedEvaluator {

    private static Logger logger = LogManager.getLogger(TimeBasedEvaluator.class);
    private final Timer newUserPreferences = MetricSystem.metrics.timer(name(TimeBasedEvaluator.class, "newUserPreferences"));

    public void evaluateTimeContext(DataModel dataModel,
                                    RecommenderBuilder builder,
                                    int incrementPreferencesBy,
                                    int maxPreferencesToUse) {

        int finallyCounter = 0;
        // Keep average metrics for this pref
        RunningAverage totalAadAverage = new FullRunningAverage();
        RunningAverage totalRmseAverage = new FullRunningAverage();


        // Keep a map of # of preferences and list of average metrics for drill down
        // Key (Integer) represents increments of preferences while the value (FastMap<Long, RunningAverage>) represents a user's running average
        List<String> averageAadsByIncrementsList = new LinkedList<String>();
        List<String> averageRmseByIncrementsList = new LinkedList<String>();

        // Get all ratings before T0 for all users and store in a data model
        FastByIDMap<PreferenceArray> dataBeforeTimeZero = getRatingsBeforeTimestamp(973018006, null);

        // Get list of users that had 76 preferences after T0
        List<Long> userIds = getSecondHalfUsers();

        try {
            // For each user u

            // Initialize a data model that will add past preferences incrementally
            //DataModel incrementalDataModel = new GenericDataModel(GenericDataModel.toDataMap(dataModel));

            double totalPreferences = 0.;
            double totalEstimatedPreferences = 0.;
            double notAbleToRecommend = 0.;


            for (Long userId : userIds) {

                RunningAverage userAadAverage = new FullRunningAverage();
                RunningAverage userRmseAverage = new FullRunningAverage();

                logger.info("Time based experiment for user " + userId);

                //Instantiate new data model for each user with data before T0
                DataModel incrementalDataModel = new GenericDataModel(dataBeforeTimeZero);

                //Get preferences for user ordered by timestamp (use SQL)
                List<Preference> userPreferences = getUserPreferencesByTimestamp(userId);

                // For each {incrementAmount} preferences p

                //Ceiling should be number that is less than userPreferencesLength, less than maxPreferencesToUse
                for (int i = 0; i < userPreferences.size(); i += incrementPreferencesBy) {
                    // Add all preferences that occurred before p
                    logger.info("i is " + i);

                    // Is this being used correctly?
                    int ceiling = calculatePreferenceCeiling(i, incrementPreferencesBy, userPreferences.size(), maxPreferencesToUse);
                    logger.info("Ceiling: " + ceiling);
                    if (ceiling == 0) {
                        break;
                    }

                    Preference firstPref = userPreferences.get(i);
                    Preference lastPref = userPreferences.get(ceiling);

                    long lowerTimestamp = dataModel.getPreferenceTime(firstPref.getUserID(), firstPref.getItemID());
                    long upperTimestamp = dataModel.getPreferenceTime(lastPref.getUserID(), lastPref.getItemID());

                    /*logger.info("User: " + userId + " first pref occurred at " + lowerTimestamp);
                    logger.info("User: " + userId + " last pref occurred at " + upperTimestamp);*/

                    //SQL select all preferences that occurred after last timestamp and before i + incrementPreferencesBy preference timestamp
                    FastByIDMap<PreferenceArray> userPreferencesBetweenTimestamps = getResultsBetweenTimestamps(lowerTimestamp, upperTimestamp);

                    //logger.info("userPreferencesBetweenTimestamp " + userPreferencesBetweenTimestamps + " Incremental data model: " + incrementalDataModel);
                    // Add all preferences that occurred over time to incrementalDataModel
                    logger.info("Users before adding to datamodel: " + incrementalDataModel.getNumUsers());
                    incrementalDataModel = addPreferencesToDatamodel(userPreferencesBetweenTimestamps, incrementalDataModel);
                    logger.info("Users after adding to datamodel: " + incrementalDataModel.getNumUsers());


                    int preferencesToEvaluate = (userPreferences.size() < maxPreferencesToUse) ? userPreferences.size() : maxPreferencesToUse;
                    logger.info("Preferences to evaluate: " + preferencesToEvaluate);
                    // Evaluate future preferences for user u
                    for (int k = ceiling; k <  preferencesToEvaluate; k++) {
                        Preference futurePref = userPreferences.get(k);
                        totalPreferences++;
                        double diff = evaluateRecommenderForUser(builder, futurePref.getUserID(), futurePref.getItemID(), incrementalDataModel, dataModel);
                        // Only count recommendations that were not NaN's or cases in which RS was unable to estimate preferences
                        if (!Double.isNaN(diff)) {
                            logger.info("Calculated difference: " + diff + " for " + userId);
                            userAadAverage.addDatum(Math.abs(diff));
                            userRmseAverage.addDatum(diff * diff);
                            //Increase # of estimated preferences for coverage
                            totalEstimatedPreferences++;
                        } else {
                            logger.info("NaN diff for user " + userId + " after " + ceiling + " preferences");
                            notAbleToRecommend++;
                        }

                    }
                    // Add averages to list for increments
                    averageAadsByIncrementsList.add(ceiling + "," + userId + "," + userAadAverage.getAverage());
                    averageRmseByIncrementsList.add(ceiling + "," + userId + "," + userRmseAverage.getAverage());

                    // Add averages to total averages
                    totalAadAverage.addDatum(userAadAverage.getAverage());
                    totalRmseAverage.addDatum((userRmseAverage.getAverage()));

                    logger.info("User " + userId + " AAD Average: " + userAadAverage.getAverage() + " RMSE Average: " + userRmseAverage.getAverage());

                }

            }

            logger.info("Total AAD Average: " + totalAadAverage.getAverage() + " Total RMSE Average: " + totalRmseAverage.getAverage());

            printList("time-context-average-aad-by-preference", averageAadsByIncrementsList);
            printList("time-context-average-rmse-by-preference", averageRmseByIncrementsList);

            logger.info("Could not recommend in " + notAbleToRecommend + " cases.");
            logger.info("Data coverage: " + (totalEstimatedPreferences / totalPreferences));

        } catch (TasteException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        } finally {
            logger.info("Reached finally " + (++finallyCounter) + " times");
            System.out.println("Reached finally " + finallyCounter + " times");

        }


    }

    public void introduceNewRatings(DataModel dataModel,
                                    RecommenderBuilder builder,
                                    int incrementPreferencesBy,
                                    int maxPreferencesToUse) {

        logger.info("Using a maximum preferences of " + maxPreferencesToUse);

        // Keep average metrics for this pref
        RunningAverage totalAadAverage = new FullRunningAverage();
        RunningAverage totalRmseAverage = new FullRunningAverage();

        // Keep a map of # of preferences and list of average metrics for drill down
        // Key (Integer) represents increments of preferences while the value (FastMap<Long, RunningAverage>) represents a user's running average
        List<String> averageAadsByIncrementsList = new LinkedList<String>();
        List<String> averageRmseByIncrementsList = new LinkedList<String>();

        //ResultSets
        ResultSet usersRs = null;

        final Timer.Context context = newUserPreferences.time();
        // Get list of preferences for user userId
        try {
            // Retrieve list of users who have over 76 preferences after T0
            /*
             *  Generate the first data model by retrieving all preferences that occurred before
             *  T0 which is, in this case, 973018006
             */

            FastByIDMap<PreferenceArray> dataBeforeTimeZero = getRatingsBeforeTimestamp(973018006, "AND user_id NOT IN (SELECT user_id FROM second_half_users) GROUP BY user_id");

            //dataBeforeTimeZero is now ready to be used to create the data model
            DataModel modelBeforeTimeZero = new GenericDataModel(dataBeforeTimeZero);

            // Now, introduce ratings from each user
            //TODO usersRs = getSecondHalfUsers();

            double totalPreferences = 0.;
            double totalEstimatedPreferences = 0.;
            double notAbleToRecommend = 0.;

            // For each user, introduce ratings in increments of incrementPreferencesBy ratings
            while (usersRs.next()) {
                long newUserId = usersRs.getLong("user_id");

                RunningAverage userAadAverage = new FullRunningAverage();
                RunningAverage userRmseAverage = new FullRunningAverage();

                logger.info("Introducing ratings for user " + newUserId);

                PreferenceArray allUsersPreferences = dataModel.getPreferencesFromUser(newUserId);

                //Increment total preference count for coverage
                //totalPreferences += allUsersPreferences.length();

                List<Preference> newUserPreferences = new LinkedList<Preference>();
                for (int i = 0; i < allUsersPreferences.length(); i += incrementPreferencesBy) {

                    logger.info("Adding " + incrementPreferencesBy + " preferences for user " + newUserId);
                    /*logger.info("i + incrementPrefencesBy: " + (i + incrementPreferencesBy));
                    logger.info("allUsersPreferences.length: " + allUsersPreferences.length());*/

                    // Add incrementPreferences # of new preferences to newUserPreferences
                    // ceiling represents the current increment. E.g 5, 10, 15, 20 if incrementPreferences is 5
                    logger.info("i + incrementPreferencesBy: " + (i + incrementPreferencesBy) + " All pref length: " + allUsersPreferences.length());
                    // If we are still within the number of user's preferences
                    int ceiling = calculatePreferenceCeiling(i, incrementPreferencesBy, allUsersPreferences.length(), maxPreferencesToUse);
                    if (ceiling == 0) {
                        break;
                    }
                    logger.info("Ceiling is " + ceiling);
                    //logger.info("Adding " + ceiling + " preferences to user " + newUserId);
                    for (int j = i; j < ceiling; j++) {
                        Preference newUserPref = allUsersPreferences.get(j);
                        newUserPreferences.add(new GenericPreference(newUserPref.getUserID(), newUserPref.getItemID(), newUserPref.getValue()));
                    }

                    // dataBeforeTimeZero now includes incrementPreference # more preferences for user newUserId
                    dataBeforeTimeZero.put(newUserId, new GenericUserPreferenceArray(newUserPreferences));
                    modelBeforeTimeZero = new GenericDataModel(dataBeforeTimeZero);
                    //TODO modelBeforeTimeZero = addPreferencesToDatamodel( ,modelBeforeTimeZero)
                    //logger.info("Data model size after adding " + modelBeforeTimeZero.getPreferencesFromUser(newUserId).length() + " preferences");

                    // For every future recommendation, compute the evaluation metrics
                    int preferencesToEvaluate = (allUsersPreferences.length() < maxPreferencesToUse) ? allUsersPreferences.length() : maxPreferencesToUse;

                    logger.info("Evaluating from " + ceiling + " to " + preferencesToEvaluate);

                    // Increment number of preferences that will have attempted to be evaluated
                    //totalPreferences += preferencesToEvaluate;

                    for (int k = ceiling; k <  preferencesToEvaluate; k++) {
                        totalPreferences++;
                        Preference futurePref = allUsersPreferences.get(k);
                        double diff = evaluateRecommenderForUser(builder, futurePref.getUserID(), futurePref.getItemID(), modelBeforeTimeZero, dataModel);
                        // Only count recommendations that were not NaN's or cases in which RS was unable to estimate preferences
                        if (!Double.isNaN(diff)) {
                            //logger.info("Calculated difference: " + diff + " for " + newUserId);
                            userAadAverage.addDatum(Math.abs(diff));
                            userRmseAverage.addDatum(diff * diff);
                            //Increase # of estimated preferences for coverage
                            totalEstimatedPreferences++;
                        } else {
                            notAbleToRecommend++;
                            logger.info("NaN diff for user " + newUserId + " after " + ceiling + " preferences");
                        }

                    }

                    averageAadsByIncrementsList.add(ceiling + "," + newUserId + "," + userAadAverage.getAverage());
                    averageRmseByIncrementsList.add(ceiling + "," + newUserId + "," + userRmseAverage.getAverage());

                }

                logger.info("User " + newUserId + " AAD Average: " + userAadAverage.getAverage() + " RMSE Average: " + userRmseAverage.getAverage());
                totalAadAverage.addDatum(userAadAverage.getAverage());
                totalRmseAverage.addDatum((userRmseAverage.getAverage()));
            }

            logger.info("Total AAD Average: " + totalAadAverage.getAverage() + " Total RMSE Average: " + totalRmseAverage.getAverage());

            printList("average-aad-by-preference", averageAadsByIncrementsList);
            printList("average-rmse-by-preference", averageRmseByIncrementsList);
            logger.info("Number of estimated predictions: " + totalEstimatedPreferences);
            logger.info("Not able to recommend: " + notAbleToRecommend);
            logger.info("Total number of predictions: " + totalPreferences);
            logger.info("Data coverage: " + (totalEstimatedPreferences / totalPreferences));


        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (TasteException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            context.stop();
            org.apache.mahout.common.IOUtils.quietClose(usersRs, null, null);
        }


    }


    /* Helper methods */

    /*
     * transform a SQL result set to a list of preferences
     */
    private List<Preference> resultSetToPreferenceList(ResultSet rs) {
        List<Preference> preferences = new LinkedList<Preference>();
        try {
            while (rs.next()) {
                long userId = rs.getLong("user_id");
                long itemId = rs.getLong("movie_id");
                int rating = rs.getInt("rating");

                preferences.add(new GenericPreference(userId, itemId, rating));
            }
            return preferences;
        } catch (SQLException e) {
            logger.error(e);
            e.printStackTrace();
        }

        return null;
    }

    /*
     * retrieve a SQL result set that contains user preferences ordered by timestamp (in chronological order)
     */
    private List<Preference> getUserPreferencesByTimestamp(long userId) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = MysqlDataSource.dataSource.getConnection();
            stmt = conn.prepareStatement("SELECT user_id, movie_id, rating FROM movie_ratings WHERE user_id = ? ORDER BY timestamp ASC",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stmt.setLong(1, userId);
            rs = stmt.executeQuery();

            return resultSetToPreferenceList(rs);

        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }

        return null;

    }

    /*
     * @param newPreferences a map containing preference arrays that have user id's and respective preferences for items
     * @param dataModel a data model into which the newPreferences will be added
     * add newPreferences to the dataModel
     */
    private DataModel addPreferencesToDatamodel(FastByIDMap<PreferenceArray> newPreferences, DataModel dataModel) {

        int preferencesAdded = 0;

        try {
            FastByIDMap<PreferenceArray> oldPreferences = GenericDataModel.toDataMap(dataModel);

            for (Map.Entry<Long, PreferenceArray> userPrefs : newPreferences.entrySet()) {
                long userId = userPrefs.getKey();
                PreferenceArray newUserPrefs = newPreferences.get(userId);
                PreferenceArray oldUserPrefs = oldPreferences.get(userId);
                // If a user's preferences are already in the old preferences, combine new prefs with old prefs
                if (oldUserPrefs != null) {
                    logger.info("oldUserPrefs not null so combining preferences");
                    logger.info("newPreferences is size " + newUserPrefs.length());
                    logger.info("oldPreferences is size " + oldUserPrefs.length());

                    List<Preference> combinedPrefList = new LinkedList<Preference>();
                    // Combine both old and new preferences into a list
                    for (Preference oldPref : oldUserPrefs) {
                        combinedPrefList.add(oldPref);
                    }
                    for (Preference newPref : newUserPrefs) {
                        combinedPrefList.add(newPref);
                        preferencesAdded++;
                    }
                    PreferenceArray combinedPrefArray = new GenericUserPreferenceArray(combinedPrefList);
                    oldPreferences.put(userId, combinedPrefArray);
                } else {
                    oldPreferences.put(userId, newUserPrefs);
                    preferencesAdded += newUserPrefs.length();
                }
            }

            return new GenericDataModel(oldPreferences);

        } catch (TasteException e) {
            logger.error(e.getMessage());
        } finally {
            logger.info(preferencesAdded + " preferences were added to the data model");
        }

        return null;
    }

    /*
     * transform a result set into a FastByIDMap<PreferenceArray> containing user preferences for items
     */
    private FastMap<Long, List<Preference>> resultsToFastMap(ResultSet rs) {

        FastMap<Long, List<Preference>> userPreferences = new FastMap<Long, List<Preference>>();
        //For each result, add to a map containing users and respective preferences
        try {
            while (rs.next()) {

                long dbUserId = rs.getLong("user_id");
                long dbMovieId = rs.getLong("movie_id");
                long dbRating = rs.getLong("rating");

                List<Preference> previousPreferences = userPreferences.get(dbUserId); //= preferencesFromDb.get(dbUserId);
                if (previousPreferences == null) {
                    previousPreferences = new LinkedList<Preference>();
                    userPreferences.put(dbUserId, previousPreferences);
                }

                previousPreferences.add(new GenericPreference(dbUserId, dbMovieId, dbRating));

            }
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }

        return userPreferences;

    }

    /*
     * transform a FastMap to FastByIDMap (usually for use with a data model)
     */
    private FastByIDMap<PreferenceArray> fastMapToFastByIdMap(FastMap<Long, List<Preference>> fastMap) {

        /* beforeTimeZero now includes all users and their list of preferences before T0;
        * loop through the map and insert into dataBeforeTimeZero
        */
        FastByIDMap<PreferenceArray> fastByIDMap = new FastByIDMap<PreferenceArray>();
        for (Map.Entry<Long, List<Preference>> entry : fastMap.entrySet()) {
            fastByIDMap.put(entry.getKey(), new GenericUserPreferenceArray(entry.getValue()));
        }

        return fastByIDMap;

    }

    /*
     *  @param lowerTimestamp the lower bound timestamp
     *  @param upperTimestamp the upper bound timestamp
     *  @return ResultSet of SQL results that are preferences that occurred between lowerTimestamp and upperTimestamp
     *
     *  retrieve preferences between two timestamps in SQL
     */
    private FastByIDMap<PreferenceArray> getResultsBetweenTimestamps(long lowerTimestamp, long upperTimestamp) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = MysqlDataSource.dataSource.getConnection();
            stmt = conn.prepareStatement("SELECT user_id, movie_id, rating FROM movie_ratings WHERE timestamp > ? AND timestamp < ?",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stmt.setLong(1, lowerTimestamp);
            stmt.setLong(2, upperTimestamp);
            rs = stmt.executeQuery();

            return fastMapToFastByIdMap(resultsToFastMap(rs));

        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }

        return null;
    }

    /*
     * @param timetamp
     * @return
     *
     * retrieve preferences that occurred before a timestamp
     */
    private FastByIDMap<PreferenceArray> getRatingsBeforeTimestamp(long timestamp, String selectGroup) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        if (selectGroup == null) {
            selectGroup = "";
        }

        try {

            conn = MysqlDataSource.dataSource.getConnection();
            stmt = conn.prepareStatement("SELECT user_id, movie_id, rating FROM movie_ratings WHERE timestamp < ? " + selectGroup + " ORDER BY timestamp ASC",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stmt.setLong(1, timestamp);
            rs = stmt.executeQuery();
            return fastMapToFastByIdMap(resultsToFastMap(rs));

        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
          org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }

        return null;
    }

    /*
     * @return
     * get all users who have rated at least 76 ratings (median amount) after half of all
     * ratings have been introduced to the data model
     */
    private List<Long> getSecondHalfUsers() {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = MysqlDataSource.dataSource.getConnection();
            stmt = conn.prepareStatement("SELECT user_id FROM second_half_users ORDER BY user_id ASC",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery();

            List<Long> userIds = new LinkedList<Long>();
            while (rs.next()) {
                long userId = rs.getLong("user_id");
                userIds.add(userId);
            }

            return userIds;

        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }
        return null;

    }

    private int calculatePreferenceCeiling(int i, int incrementPreferencesBy, int allUsersPreferences, int maxPreferencesToUse) {
        int ceiling = 0;
        if (i + incrementPreferencesBy < allUsersPreferences) {
            // Increment ceiling only if it is within max preferences to use
            if (i + incrementPreferencesBy < maxPreferencesToUse) {
                ceiling = i + incrementPreferencesBy;
            }

        }
        return ceiling;

    }


    /*
     * print a list to a CSV file
     */
    private void printList(String s, List<String> averageList) {
        logger.info(s);
        try {
            PrintWriter writer = new PrintWriter(new FileWriter(new File("logs/"+s+"-average-list.csv")));
            for (String avgString : averageList) {
                writer.println(avgString);
            }
            writer.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    private double evaluateRecommenderForUser(RecommenderBuilder builder, long userId, long itemId, DataModel dataModelWithoutUser, DataModel dataModelWithUser) {

        try {
            logger.info("Evaluating future preference for user " + userId + " on item " + itemId);
            logger.info("DataModel without preference is: " + dataModelWithoutUser.getNumUsers());
            logger.info("DataModel with preference is: " + dataModelWithUser.getNumUsers());

            Recommender recommender = builder.buildRecommender(dataModelWithoutUser);
            double estimatedPreference = recommender.estimatePreference(userId, itemId);
            if (Double.isNaN(estimatedPreference)) {
                //logger.info("The estimated preference was NaN in evaluateRecommenderForUser method");
                return Double.NaN;
            }
            double actualPreference = dataModelWithUser.getPreferenceValue(userId, itemId);
            double diff = actualPreference - estimatedPreference;

            //logger.info("Estimated: " + estimatedPreference + " Actual: " + actualPreference + " Difference: " + diff);
            return diff;

        } catch (TasteException e) {
            logger.error("Error: " + e.getMessage());
        }
        //logger.info("Reached end of evaluateRecommenderForuser; returning NaN");

        return Double.NaN;


    }

}
