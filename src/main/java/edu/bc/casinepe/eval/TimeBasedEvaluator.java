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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by petercasinelli on 3/29/14.
 */
public class TimeBasedEvaluator {

    private static Logger logger = LogManager.getLogger(TimeBasedEvaluator.class);
    private final Timer newUserPreferences = MetricSystem.metrics.timer(name(TimeBasedEvaluator.class, "newUserPreferences"));

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

        final Timer.Context context = newUserPreferences.time();
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

            long firstTimestamp = Long.MAX_VALUE;
            while (rs.next()) {

                long timestamp = rs.getLong("timestamp");
                if (timestamp < firstTimestamp) {
                    firstTimestamp =  timestamp;
                }
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

            // Get all ratings from other users that rated items before current user
            /*
            // Code for using SQL rather than DataModel
            conn = MysqlDataSource.getMysqlDataSource().getConnection();
            stmt = conn.prepareStatement("SELECT user_id, movie_id, timestamp, rating FROM movie_ratings WHERE timestamp <= ? AND user_id != ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setLong(1, firstTimestamp);
            stmt.setLong(2, userId);

            rs = stmt.executeQuery();

            // Get all preferences in DataModel BESIDES preferences from userId and put them in preferencesFromDb
            FastMap<Long, List<Preference>> preferencesFromDb = new FastMap<Long, List<Preference>>();
            while (rs.next()) {

                long dbUserId = rs.getLong("user_id");

                if (dbUserId == userId) {
                    continue;
                }

                if (random.nextDouble() >= evaluationPercentage) {
                    //Skipped
                    continue;
                }

                long dbMovieId = rs.getLong("movie_id");
                long dbRating = rs.getLong("rating");

                List<Preference> previousPreferences = preferencesFromDb.get(dbUserId); //= preferencesFromDb.get(dbUserId);
                if (previousPreferences == null) {
                    previousPreferences = new LinkedList<Preference>();
                    preferencesFromDb.put(dbUserId, previousPreferences);
                }

                previousPreferences.add(new GenericPreference(dbUserId, dbMovieId, dbRating));
            }
            */
            FastByIDMap<PreferenceArray> allOtherPreferencesSubset = new FastByIDMap<PreferenceArray>();
            /*for (Map.Entry<Long, List<Preference>> entry : preferencesFromDb.entrySet()) {
                allOtherPreferencesSubset.put(entry.getKey(), new GenericUserPreferenceArray(entry.getValue()));
            } */



            LongPrimitiveIterator allUsers = dataModel.getUserIDs();
            /* For each user that has at least the median number of preferences, add that median number
               of preferences to allOtherPreferencesSubset
             */
            while (allUsers.hasNext()) {
                long nextUserId = allUsers.nextLong();

                PreferenceArray nextUsersPreferences = dataModel.getPreferencesFromUser(nextUserId);
                // Median for 1mm set is 96
                if (nextUsersPreferences.length() >= 96) {

                    // do not add this userId's preferences; should happen once
                    if (nextUserId == userId) {
                        continue;
                    }

                    /*if (random.nextDouble() >= evaluationPercentage) {
                        // Skipped
                        continue;
                    }*/

                    PreferenceArray nextUsersPreferencesSubset = new GenericUserPreferenceArray(96);
                    // Count should be below 0
                    int count = 0;
                    for (Preference nextUsersPref : nextUsersPreferences) {

                        if (dataModel.getPreferenceTime(nextUsersPref.getUserID(), nextUsersPref.getItemID()) <= firstTimestamp) {

                        }

                    }

                    for (int i = 0; i < 96; i++) {
                        nextUsersPreferencesSubset.set(i, nextUsersPreferences.get(i));
                    }


                    //PreferenceArray nextUsersPreferences = dataModel.getPreferencesFromUser(nextUserId);
                    allOtherPreferencesSubset.put(nextUserId, nextUsersPreferencesSubset);
                }

            }

            // Now allOtherPreferencesModel contains all preferences that occurred before userId's
            // first preference
            DataModel allOtherPreferencesDataModel = new GenericDataModel(allOtherPreferencesSubset);

            PreferenceArray incrementalUserPreferences = new GenericUserPreferenceArray(userPrefs.size());
            // Introduce one preference at a time for userId into allOtherPreferencesModel
            for (int i = 0; i < prefs.length(); i += 5) {

                RunningAverage aadAverage = new FullRunningAverage();
                RunningAverage rmseAverage = new FullRunningAverage();

                //Get next preference
                Preference pref = prefs.get(i);

                /*System.out.println("Introducing a rating from userId: "
                        + pref.getUserID() + " Item: "
                        + pref.getItemID() + " Val: " + pref.getValue());*/

                // Introduce the preference to data model
                incrementalUserPreferences.set(i, new GenericPreference(pref.getUserID(), pref.getItemID(), pref.getValue()));
                allOtherPreferencesSubset.put(pref.getUserID(), incrementalUserPreferences);

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
            context.stop();
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }

        return new double[]{totalAadAverage.getAverage(), totalRmseAverage.getAverage()};
    }

    public void introduceNewRatings(DataModel dataModel,
                                    RecommenderBuilder builder,
                                    int incrementPreferencesBy) {

        int maxPreferencesToUse = 76;
        logger.info("Using a maximum preferences of " + maxPreferencesToUse);

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        // Keep average metrics for this pref
        RunningAverage totalAadAverage = new FullRunningAverage();
        RunningAverage totalRmseAverage = new FullRunningAverage();

        // Keep a map of # of preferences and list of average metrics for drill down
        // Key (Integer) represents increments of preferences while the value (FastMap<Long, RunningAverage>) represents a user's running average
        List<String> averageAadsByIncrementsList = new LinkedList<String>();
        List<String> averageRmseByIncrementsList = new LinkedList<String>();

        final Timer.Context context = newUserPreferences.time();
        // Get list of preferences for user userId
        try {
            // Retrieve list of users who have over 76 preferences after T0
            /*
             *  Generate the first data model by retrieving all preferences that occurred before
             *  T0 which is, in this case, 973018006
             */
            conn = MysqlDataSource.getMysqlDataSource().getConnection();
            stmt = conn.prepareStatement("SELECT user_id, movie_id, rating FROM movie_ratings WHERE timestamp < 973018006 AND user_id NOT IN (SELECT user_id FROM second_half_users) GROUP BY user_id ORDER BY timestamp ASC",
                                         ResultSet.TYPE_FORWARD_ONLY,
                                         ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery();

            FastMap<Long, List<Preference>> beforeTimeZero = new FastMap<Long, List<Preference>>();
            while (rs.next()) {

                long dbUserId = rs.getLong("user_id");
                long dbMovieId = rs.getLong("movie_id");
                long dbRating = rs.getLong("rating");

                List<Preference> previousPreferences = beforeTimeZero.get(dbUserId); //= preferencesFromDb.get(dbUserId);
                if (previousPreferences == null) {
                    previousPreferences = new LinkedList<Preference>();
                    beforeTimeZero.put(dbUserId, previousPreferences);
                }

                previousPreferences.add(new GenericPreference(dbUserId, dbMovieId, dbRating));

            }

            /* beforeTimeZero now includes all users and their list of preferences before T0;
             * loop through the map and insert into dataBeforeTimeZero
             */
            FastByIDMap<PreferenceArray> dataBeforeTimeZero = new FastByIDMap<PreferenceArray>();
            for (Map.Entry<Long, List<Preference>> entry : beforeTimeZero.entrySet()) {
                dataBeforeTimeZero.put(entry.getKey(), new GenericUserPreferenceArray(entry.getValue()));
            }

            //dataBeforeTimeZero is now ready to be used to create the data model
            DataModel modelBeforeTimeZero = new GenericDataModel(dataBeforeTimeZero);

            // Now, introduce ratings from each user
            conn = MysqlDataSource.getMysqlDataSource().getConnection();
            stmt = conn.prepareStatement("SELECT user_id FROM second_half_users ORDER BY user_id ASC",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery();

            double totalPreferences = 0.;
            double totalEstimatedPreferences = 0.;

            // For each user, introduce ratings in increments of incrementPreferencesBy ratings
            while (rs.next()) {
                long newUserId = rs.getLong("user_id");

                RunningAverage userAadAverage = new FullRunningAverage();
                RunningAverage userRmseAverage = new FullRunningAverage();

                logger.info("Introducing ratings for user " + newUserId);

                PreferenceArray allUsersPreferences = dataModel.getPreferencesFromUser(newUserId);

                //Increment total preference count for coverage
                totalPreferences += allUsersPreferences.length();

                List<Preference> newUserPreferences = new LinkedList<Preference>();
                for (int i = 0; i < allUsersPreferences.length(); i += incrementPreferencesBy) {

                    logger.info("Adding " + incrementPreferencesBy + " preferences for user " + newUserId);
                    /*logger.info("i + incrementPrefencesBy: " + (i + incrementPreferencesBy));
                    logger.info("allUsersPreferences.length: " + allUsersPreferences.length());*/

                    // Add incrementPreferences # of new preferences to newUserPreferences
                    // ceiling represents the current increment. E.g 5, 10, 15, 20 if incrementPreferences is 5
                    int ceiling = 0;
                    logger.info("i + incrementPreferencesBy: " + (i + incrementPreferencesBy) + " All pref length: " + allUsersPreferences.length());

                    // If we are still within the number of user's preferences
                    if (i + incrementPreferencesBy < allUsersPreferences.length()) {
                        // Increment ceiling only if it is within max preferences to use
                        if (i + incrementPreferencesBy < maxPreferencesToUse) {
                            ceiling = i + incrementPreferencesBy;
                        } else {
                            break;
                        }

                    } else {
                        break;
                    }

                    //logger.info("Adding " + ceiling + " preferences to user " + newUserId);
                    for (int j = i; j < ceiling; j++) {
                        Preference newUserPref = allUsersPreferences.get(j);
                        newUserPreferences.add(new GenericPreference(newUserPref.getUserID(), newUserPref.getItemID(), newUserPref.getValue()));
                    }

                    // dataBeforeTimeZero now includes incrementPreference # more preferences for user newUserId
                    dataBeforeTimeZero.put(newUserId, new GenericUserPreferenceArray(newUserPreferences));
                    modelBeforeTimeZero = new GenericDataModel(dataBeforeTimeZero);
                    //logger.info("Data model size after adding " + modelBeforeTimeZero.getPreferencesFromUser(newUserId).length() + " preferences");

                    // For every future recommendation, compute the evaluation metrics
                    int preferencesToEvaluate = (allUsersPreferences.length() < maxPreferencesToUse) ? allUsersPreferences.length() : maxPreferencesToUse;
                    logger.info("Evaluating from " + ceiling + " to " + preferencesToEvaluate);

                    for (int k = ceiling; k <  preferencesToEvaluate; k++) {
                        Preference futurePref = allUsersPreferences.get(k);
                        double diff = evaluateRecommenderForUser(builder, futurePref.getUserID(), futurePref.getItemID(), modelBeforeTimeZero, dataModel);
                        // Only count recommendations that were not NaN's or cases in which RS was unable to estimate preferences
                        if (!Double.isNaN(diff)) {
                            //logger.info("Calculated difference: " + diff + " for " + newUserId);
                            userAadAverage.addDatum(Math.abs(diff));
                            userRmseAverage.addDatum(diff * diff);
                            //Increase # of estimated preferences for coverage
                            totalEstimatedPreferences++;
                        }/* else {
                            logger.info("NaN diff for user " + newUserId + " after " + ceiling + " preferences");
                        }*/

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

            logger.info("Data coverage: " + (totalEstimatedPreferences / totalPreferences));


        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } catch (TasteException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        } finally {
            context.stop();
        }


    }

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

    private void printMap(String s, FastMap<Integer, FastMap<Long, RunningAverage>> averageByIncrements) {
        logger.info(s);
        // entry's key is # of increments and value is a map containing a user and their running average value
        for (FastMap.Entry<Integer, FastMap<Long, RunningAverage>> entry : averageByIncrements.entrySet()) {
            int ratingsIntroduced = entry.getKey();
            logger.info(ratingsIntroduced + " ratings were introduced");


            // userEntry's key is user id and value is a running average
            for (FastMap.Entry<Long, RunningAverage> userEntry : entry.getValue().entrySet()) {
                long userId = userEntry.getKey();
                double runningAverage = userEntry.getValue().getAverage();
                logger.info(ratingsIntroduced + "," + userId + "," + runningAverage);
            }
        }
    }

    private double evaluateRecommenderForUser(RecommenderBuilder builder, long userId, long itemId, DataModel dataModelWithoutUser, DataModel dataModelWithUser) {
        //logger.info("Evaluating future preference for user " + userId + " on item " + itemId);
        try {
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
