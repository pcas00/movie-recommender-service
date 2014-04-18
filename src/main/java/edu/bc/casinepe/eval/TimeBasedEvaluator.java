package edu.bc.casinepe.eval;

import com.codahale.metrics.Timer;
import edu.bc.casinepe.jdbc.MysqlDataSource;
import edu.bc.casinepe.metrics.MetricSystem;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static Logger logger = LoggerFactory.getLogger(TimeBasedEvaluator.class);
    private final Timer newUserPreferences = MetricSystem.metrics.timer(name(TimeBasedEvaluator.class, "newUserPreferences"));

    public void introduceNewRatings(DataModel dataModel,
                                    RecommenderBuilder builder,
                                    String similarityStrategy,
                                    int incrementPreferencesBy,
                                    int maxPreferencesToUse,
                                    double trainingPercentage,
                                    double evaluationPercentage) {

        logger.info("Using a maximum preferences of " + maxPreferencesToUse);

        // calculate training and evaluation
        int trainingNumber = (int) (maxPreferencesToUse * trainingPercentage);
        int evaluationNumber = (int) (maxPreferencesToUse * evaluationPercentage);

        // Keep average metrics for this pref
        RunningAverage totalAadAverage = new FullRunningAverage();
        RunningAverage totalRmseAverage = new FullRunningAverage();

        // Keep list of average metrics per user represented by a string
        List<String> averageAadsByIncrementsList = new LinkedList<String>();
        List<String> averageRmseByIncrementsList = new LinkedList<String>();


        final Timer.Context context = newUserPreferences.time();
        // Get list of preferences for user userId
        try {

            LongPrimitiveIterator userIDs = dataModel.getUserIDs();

            int userCount = 0;
            // For each user, introduce ratings in increments of incrementPreferencesBy ratings

            while (userIDs.hasNext()) {

                userCount++;
                long newUserId = userIDs.nextLong();

                RunningAverage userAadAverage = new FullRunningAverage();
                RunningAverage userRmseAverage = new FullRunningAverage();


                // Get user newUserId' preferences ordered by timestamp ASC
                List<Preference> userPreferences = getUserPreferencesByTimestamp(newUserId);

                Preference firstPreference = userPreferences.get(0);

                long firstTimestamp = getPreferenceTimestamp(newUserId, firstPreference.getItemID());
                List<Preference> preferencesBeforeTimeZero = getRatingsByTimestamp(firstTimestamp, '<', null);
                logger.info("There are " + preferencesBeforeTimeZero.size() + " preferences before " + firstTimestamp);

                logger.info("Introducing ratings for user " + newUserId + " who is user " + (userCount));
                //incrementalDataModel is now ready to be used to create the data model
                DataModel incrementalDataModel = new GenericDataModel(listToFastByIDMap(preferencesBeforeTimeZero));

                // Calculate the max number of preferences to evaluate for user
                //double maxPreferencesForUser = (trainingPercentage > userPreferences.size()) ? userPreferences.size() * 0.8 : trainingPercentage;
                int maxPreferencesForUser = trainingNumber;

                //logger.info("Max preferences to use for this user: " + trainingPercentage + " User Preferences Size: " + userPreferences.size());
                logger.info("Will introduce " + maxPreferencesForUser + " preferences from user " + newUserId);

                //TODO should this be done only within the next loop? are duplicate ratings be added?
                List<Preference> newPreferencesToIntroduce = new ArrayList(maxPreferencesForUser + 1);

                for (int i = incrementPreferencesBy; i <= maxPreferencesForUser; i += incrementPreferencesBy) {
                    double totalPreferences = 0.;
                    double totalEstimatedPreferences = 0.;

                    logger.info("New preferences size is " + newPreferencesToIntroduce.size());
                    // Add incrementPreferencesBy preferences from dataBeforeTimeZero
                    logger.info("Add from " + (i - incrementPreferencesBy) + " to " + (i));

                    for (int j = i - incrementPreferencesBy; j < i; j++) {
                        newPreferencesToIntroduce.add(userPreferences.get(j));
                    }
                    logger.info("User " + newUserId + " now has " + newPreferencesToIntroduce.size() + " preferences as training data");

                    // Add new preferences to incrementalDataModel
                    incrementalDataModel = addPreferencesToDatamodel(listToFastByIDMap(newPreferencesToIntroduce), incrementalDataModel);
                    //logger.info("Evaluate from " + (i + incrementPreferencesBy) + " to " + maxPreferencesForUser);

                    //Evaluate on evaluation percentage
                    logger.info("Evaluate from " + (maxPreferencesForUser + 1) + " to " + (maxPreferencesForUser + evaluationNumber));
                    for (int k = maxPreferencesForUser + 1; k < maxPreferencesForUser + evaluationNumber; k++) {
                        totalPreferences++;
                        Preference futurePref = userPreferences.get(k);
                        double diff = evaluateRecommenderForUser(builder, futurePref.getUserID(), futurePref.getItemID(), incrementalDataModel, dataModel);

                        // Only count recommendations that were not NaN's or cases in which RS was unable to estimate preferences
                        if (!Double.isNaN(diff)) {
                            logger.info("Calculated difference: " + diff + " for " + newUserId + " on " + futurePref.getItemID());
                            userAadAverage.addDatum(Math.abs(diff));
                            userRmseAverage.addDatum(diff * diff);

                            //Increase # of estimated preferences for coverage
                            totalEstimatedPreferences++;
                        } else {
                            logger.info("NaN diff for user " + newUserId + " with item " + futurePref.getItemID() + " after " + i + " preferences");
                        }

                    }
                    logger.info("Current coverage: " + (totalEstimatedPreferences / totalPreferences));


                    // Add an average value for each user after each increment
                    if (!Double.isNaN(userAadAverage.getAverage())) {
                        averageAadsByIncrementsList.add(i + "," + newUserId + "," + userAadAverage.getAverage() + "," + (totalEstimatedPreferences / totalPreferences));
                    }

                    if (!Double.isNaN(userRmseAverage.getAverage())) {
                        averageRmseByIncrementsList.add(i + "," + newUserId + "," + userRmseAverage.getAverage() + "," + (totalEstimatedPreferences / totalPreferences));
                    }


                }


                logger.info("User " + newUserId + " AAD Average: " + userAadAverage.getAverage() + " RMSE Average: " + userRmseAverage.getAverage());

                if (!Double.isNaN(userAadAverage.getAverage())) {
                    totalAadAverage.addDatum(userAadAverage.getAverage());
                } else {
                    logger.info("Not adding user " + newUserId + " AAD averages because they are NaN");
                }

                if (!Double.isNaN(userRmseAverage.getAverage())) {
                    totalRmseAverage.addDatum((userRmseAverage.getAverage()));
                } else {
                    logger.info("Not adding user " + newUserId + " RMSE averages because they are NaN");
                }
            }

            logger.info("Total AAD Average: " + totalAadAverage.getAverage() + " Total RMSE Average: " + totalRmseAverage.getAverage());

            printList(similarityStrategy + "-average-aad-by-preference", averageAadsByIncrementsList);
            printList(similarityStrategy + "-average-rmse-by-preference", averageRmseByIncrementsList);

            /*logger.info("Number of estimated predictions: " + totalEstimatedPreferences);
            logger.info("Not able to recommend: " + notAbleToRecommend);
            logger.info("Total number of predictions: " + totalPreferences);
            logger.info("Data coverage: " + (totalEstimatedPreferences / totalPreferences));*/


        } catch (TasteException e) {
            e.printStackTrace();
        } finally {
            context.stop();
        }


    }



    public void introduceNonTargetDataIncrements(DataModel dataModel,
                                                 RecommenderBuilder builder,
                                                 String similarityStrategy,
                                                 int incrementPreferencesBy,
                                                 int maxPreferencesToIntroduce,
                                                 double trainingPercentage,
                                                 double evaluationPercentage) {


        // Keep average metrics for this pref
        RunningAverage totalAadAverage = new FullRunningAverage();
        RunningAverage totalRmseAverage = new FullRunningAverage();


        // Keep a map of # of preferences and list of average metrics for drill down
        // Key (Integer) represents increments of preferences while the value (FastMap<Long, RunningAverage>) represents a user's running average
        List<String> averageAadsByIncrementsList = new LinkedList<String>();
        List<String> averageRmseByIncrementsList = new LinkedList<String>();

        // Get all ratings before T0 for all users and store in a data model
        //List<Preference> dataBeforeTimeZero = getRatingsByTimestamp(973018006, '<', "AND user_id NOT IN (SELECT user_id FROM second_half_users)");
        //TODO List<Preference> dataBeforeTimeZero = getRatingsByTimestamp(973018006, '<', null);


        //Randomize order of preferences
        //Collections.shuffle(dataBeforeTimeZero);
        //TODO logger.info("There are " + dataBeforeTimeZero.size() + " ratings that occurred before T0");

        //Load a data model with all preferences between T0
        //TODO DataModel incrementalDataModel = new GenericDataModel(listToFastByIDMap(dataBeforeTimeZero));

        // Get list of users that had 76 (median #) preferences after T0
        //TODO List<Long> userIds = getHalfOfUsers("second");

        try {

            LongPrimitiveIterator userIds = dataModel.getUserIDs();
            int userCount = 0;
            // For each user
            while (userIds.hasNext()) {
                userCount++;

                long userId = userIds.nextLong();

                logger.info("Evaluating non target data added for user " + userId + " who is user: " + userCount);

                //Keep running averages for each user
                RunningAverage userAadAverage = new FullRunningAverage();
                RunningAverage userRmseAverage = new FullRunningAverage();

                List<Preference> userPreferences = getUserPreferencesByTimestamp(userId);

                int numberOfUserPrefs = userPreferences.size();
                logger.info("This user has a total of " + numberOfUserPrefs + " preferences");

                // Now separate userPreferences into training and evaluation data
                int ratingsToTrain = (int) (numberOfUserPrefs * trainingPercentage);
                int ratingsToEvaluate = (int) (numberOfUserPrefs * evaluationPercentage);

                /*
                 Add all ratings that occurred before user u's first rating and after user u's last rating
                 from the shuffled list userPreferences
                 */
                Preference firstPref = userPreferences.get(0);
                Preference lastPref = userPreferences.get(ratingsToTrain - 1);
                logger.info("firstPref: " + firstPref);
                logger.info("lastPref: " + lastPref);

                long firstTimestamp = this.getPreferenceTimestamp(firstPref.getUserID(), firstPref.getItemID());
                long lastTimestamp = this.getPreferenceTimestamp(lastPref.getUserID(), lastPref.getItemID());

                List<Preference> dataBeforeFirstTimestamp = getRatingsByTimestamp(firstTimestamp, '<', null);
                List<Preference> dataAfterLastTimestamp = getRatingsByTimestamp(lastTimestamp, '>', null);

                //Add training data to all preferences to choose from
                List<Preference> allPreferencesToChooseFrom = new ArrayList<Preference>(maxPreferencesToIntroduce);

                logger.info("All preferences before timestamp: " + dataBeforeFirstTimestamp.size());
                logger.info("All preferences after timestamp: " + dataAfterLastTimestamp.size());
                if (dataBeforeFirstTimestamp != null) {
                    allPreferencesToChooseFrom.addAll(dataBeforeFirstTimestamp);
                }
                if (dataAfterLastTimestamp != null) {
                    allPreferencesToChooseFrom.addAll(dataAfterLastTimestamp);
                }

                Collections.shuffle(allPreferencesToChooseFrom);
                Collections.shuffle(userPreferences);
                /* Now allPreferencesToChooseFrom contains (shuffled) all of the preferences from which to poll
                   preferences in increments
                 */

                //Training data
                logger.info("Adding training preferences from 0 to " + ratingsToTrain + " to data model");
                List<Preference> trainingPreferences = new ArrayList<Preference>(ratingsToTrain);
                for (int i = 0; i < ratingsToTrain; i++) {
                    trainingPreferences.add(userPreferences.get(i));
                }

                //Evaluation data
                logger.info("Adding evaluation preferences from " + ratingsToTrain + " to " + (ratingsToTrain + ratingsToEvaluate));
                List<Preference> evaluationPreferences = new ArrayList<Preference>(ratingsToTrain + ratingsToEvaluate);
                for (int i = ratingsToTrain; i < ratingsToTrain + ratingsToEvaluate; i++) {
                    evaluationPreferences.add(userPreferences.get(i));
                }


                // Create a data model that originally contains user's training data
                // this data model will be incremented with more training data
                DataModel usersIncrementalDataModel = new GenericDataModel(listToFastByIDMap(trainingPreferences));



                // Add increments of random preferences from allPreferencesToChooseFrom for this user
                for (int i = incrementPreferencesBy; i <= maxPreferencesToIntroduce/*allPreferencesToChooseFrom.size() - 1*/; i += incrementPreferencesBy) {

                    //Keep track of coverage per increment
                    double totalPreferences = 0.;
                    double estimatedPreferences = 0.;

                    // Add incrementPreferencesBy preferences from dataBeforeTimeZero
                    List<Preference> newPreferencesToIntroduce = new ArrayList(incrementPreferencesBy);

                    int ceiling = (allPreferencesToChooseFrom.size() < i) ? allPreferencesToChooseFrom.size() : i;
                    //logger.info("j is " + (i - incrementPreferencesBy) + " and j must be < " + ceiling);
                    logger.info("allPreferencesToChooseFrom: " + allPreferencesToChooseFrom.size());
                    logger.info("Adding preferences from " + (i - incrementPreferencesBy) + " to " + ceiling);
                    for (int j = i - incrementPreferencesBy; j < ceiling; j++) {
                        newPreferencesToIntroduce.add(allPreferencesToChooseFrom.get(j));
                    }

                    logger.info(newPreferencesToIntroduce.size() + " are being introduced for all users");
                    usersIncrementalDataModel = addPreferencesToDatamodel(listToFastByIDMap(newPreferencesToIntroduce), usersIncrementalDataModel);

                    // Evaluate on user's evaluation preferences
                    logger.info("Evaluating on " + evaluationPreferences.size() + " evaluation preferences");
                    for (Preference evaluationPreference : evaluationPreferences) {

                        totalPreferences = totalPreferences + 1.;

                        double diff = evaluateRecommenderForUser(builder, evaluationPreference.getUserID(), evaluationPreference.getItemID(), usersIncrementalDataModel, dataModel);
                        // Only count recommendations that were not NaN's or cases in which RS was unable to estimate preferences
                        if (!Double.isNaN(diff)) {
                            logger.info("Calculated difference: " + diff + " for " + userId);
                            userAadAverage.addDatum(Math.abs(diff));
                            userRmseAverage.addDatum(diff * diff);

                            //Increase # of estimated preferences for coverage
                            estimatedPreferences = estimatedPreferences + 1.;

                        } else {
                            logger.info("NaN diff for user " + userId + " after " + i + " preferences");
                        }

                    }

                    // Add averages to list for increments if they are not NaN
                    if (!Double.isNaN(userAadAverage.getAverage())) {
                        // Add averages to total averages
                        averageAadsByIncrementsList.add(i + "," + userId + "," + userAadAverage.getAverage() + "," + (estimatedPreferences / totalPreferences));
                        totalAadAverage.addDatum(userAadAverage.getAverage());
                    }

                    if (!Double.isNaN(userRmseAverage.getAverage())) {
                        averageRmseByIncrementsList.add(i + "," + userId + "," + userRmseAverage.getAverage() + "," + (estimatedPreferences / totalPreferences));
                        totalRmseAverage.addDatum((userRmseAverage.getAverage()));
                    }


                }

                logger.info("User " + userId + " AAD Average: " + userAadAverage.getAverage() + " RMSE Average: " + userRmseAverage.getAverage());
                printList(similarityStrategy + "-non-target-aad", averageAadsByIncrementsList);
                printList(similarityStrategy + "-non-target-rmse", averageRmseByIncrementsList);

            }

            logger.info("Total AAD Average: " + totalAadAverage.getAverage() + " Total RMSE Average: " + totalRmseAverage.getAverage());


        } catch (TasteException e) {
            e.printStackTrace();
        }






        /*
        // Add 50 preferences from all users
        FastByIDMap<PreferenceArray> secondHalfUsersRatings = new FastByIDMap<PreferenceArray>();
        for (long userId : userIds) {
            List<Preference> userPreferences = getUserPreferencesByTimestamp(userId);
            List<Preference> userPreferenceSubset = new ArrayList(50);
            for (int i = 0; i < 50; i++) {
                userPreferenceSubset.add(userPreferences.get(i));
            }
            logger.info("Add 50 preferences for " + userId);
            PreferenceArray userPrefArray = new GenericUserPreferenceArray(userPreferenceSubset);
            secondHalfUsersRatings.put(userId, userPrefArray);
        }

        //Add 50 ratings from users who rated median after 76 ratings after T0
        incrementalDataModel = addPreferencesToDatamodel(secondHalfUsersRatings, incrementalDataModel);

        double totalPreferences = 0.;
        double totalEstimatedPreferences = 0.;
        double notAbleToRecommend = 0.;

        //Add 1000 ratings before T0 that do not include second half users to an incremental data model

        // For all ratings that occurred before T0 and do not include any user from userIds
        logger.info("Data size is " + dataBeforeTimeZero.size());
        for (int i = incrementPreferencesBy; i < dataBeforeTimeZero.size() - 1; i += incrementPreferencesBy) {

            // Add incrementPreferencesBy preferences from dataBeforeTimeZero
            List<Preference> newPreferencesToIntroduce = new ArrayList(incrementPreferencesBy);

            int ceiling = (dataBeforeTimeZero.size() < i + incrementPreferencesBy) ? dataBeforeTimeZero.size() : i + incrementPreferencesBy;
            logger.info("j is " + (i - incrementPreferencesBy) + " and j must be < " + ceiling);

            for (int j = i - incrementPreferencesBy; j < ceiling; j++) {
                newPreferencesToIntroduce.add(dataBeforeTimeZero.get(j));
            }
            logger.info(newPreferencesToIntroduce.size() + " have been introduced for all users");
            incrementalDataModel = addPreferencesToDatamodel(listToFastByIDMap(newPreferencesToIntroduce), incrementalDataModel);

            //Evaluate all users who have 76 ratings after T0 using the incremental data model

            //For each user
            for (Long userId : userIds) {

                RunningAverage userAadAverage = new FullRunningAverage();
                RunningAverage userRmseAverage = new FullRunningAverage();

                // Get their preferences that occurred after T0
                List<Preference> userPreferences = getUserPreferencesByTimestamp(userId);

                //Evaluating from 50 to 70
                for (int pref = 50; pref < 76; pref++) {

                    Preference futurePref = userPreferences.get(pref);

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
                        logger.info("NaN diff for user " + userId + " after " + i + " preferences");
                        notAbleToRecommend++;
                    }

                }


                // Add averages to list for increments if they are not NaN
                if (!Double.isNaN(userAadAverage.getAverage())) {
                    // Add averages to total averages
                    averageAadsByIncrementsList.add(i + "," + userId + "," + userAadAverage.getAverage() + "," + (totalEstimatedPreferences / totalPreferences));
                    totalAadAverage.addDatum(userAadAverage.getAverage());
                }

                if (!Double.isNaN(userRmseAverage.getAverage())) {
                    averageRmseByIncrementsList.add(i + "," + userId + "," + userRmseAverage.getAverage() + "," + (totalEstimatedPreferences / totalPreferences));
                    totalRmseAverage.addDatum((userRmseAverage.getAverage()));
                }

                logger.info("Coverage is " + (totalEstimatedPreferences / totalPreferences));
                logger.info("User " + userId + " AAD Average: " + userAadAverage.getAverage() + " RMSE Average: " + userRmseAverage.getAverage());



            }


        }

        logger.info("Total AAD Average: " + totalAadAverage.getAverage() + " Total RMSE Average: " + totalRmseAverage.getAverage());

        printList(similarityStrategy + "-non-target-dataset-average-aad-by-increment", averageAadsByIncrementsList);
        printList(similarityStrategy + "-non-target-dataset-average-rmse-by-increment", averageRmseByIncrementsList);

        logger.info("Could not recommend in " + notAbleToRecommend + " cases.");
        logger.info("Data coverage: " + (totalEstimatedPreferences / totalPreferences));
        */


    }



    private long getPreferenceTimestamp(long userId, long itemId) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = MysqlDataSource.dataSource.getConnection();
            stmt = conn.prepareStatement("SELECT timestamp FROM movie_ratings WHERE user_id = ? AND movie_id = ? LIMIT 1",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stmt.setLong(1, userId);
            stmt.setLong(2, itemId);

            rs = stmt.executeQuery();
            while (rs.next()) {
                return rs.getLong("timestamp");
            }

        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }

        return -1;
    }


    /* Helper methods */

    /*
     * transform a SQL result set to a list of preferences
     */
    private List<Preference> resultSetToPreferenceList(ResultSet rs) {
        List<Preference> preferences = new ArrayList<Preference>();
        try {
            while (rs.next()) {
                long userId = rs.getLong("user_id");
                long itemId = rs.getLong("movie_id");
                int rating = rs.getInt("rating");

                preferences.add(new GenericPreference(userId, itemId, rating));
            }
            return preferences;
        } catch (SQLException e) {
            logger.error(e.getMessage());
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

                    List<Preference> combinedPrefList = new ArrayList<Preference>();
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

            //logger.info(preferencesAdded + " preferences were added to datamodel");


            return new GenericDataModel(oldPreferences);

        } catch (TasteException e) {
            logger.error(e.getMessage());
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

    private FastByIDMap<PreferenceArray> listToFastByIDMap(List<Preference> preferences) {

        return fastMapToFastByIdMap(listToFastMap(preferences));

    }

    private FastMap<Long, List<Preference>> listToFastMap(List<Preference> preferences) {

        FastMap<Long, List<Preference>> userPreferences = new FastMap<Long, List<Preference>>();
        //For each preference, add to a map containing users and respective preferences
        for (Preference pref : preferences) {

            long userId = pref.getUserID();

            List<Preference> previousPreferences = userPreferences.get(userId); //= preferencesFromDb.get(dbUserId);
            if (previousPreferences == null) {
                previousPreferences = new LinkedList<Preference>();
                userPreferences.put(userId, previousPreferences);
            }

            previousPreferences.add(pref);

        }

        return userPreferences;

    }

    private List<Preference> getRatingsByTimestamp(long timestamp, char comparison, String selectGroup) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        if (selectGroup == null) {
            selectGroup = "";
        }

        try {

            conn = MysqlDataSource.dataSource.getConnection();
            stmt = conn.prepareStatement("SELECT user_id, movie_id, rating FROM movie_ratings WHERE timestamp " + comparison + " ? " + selectGroup,
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            stmt.setLong(1, timestamp);
            rs = stmt.executeQuery();
            return resultsToList(rs);

        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }

        return null;
    }

    private List<Preference> getAllRatingsExcept(String exceptGroup) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        if (exceptGroup == null) {
            exceptGroup = "";
        }

        try {

            conn = MysqlDataSource.dataSource.getConnection();
            stmt = conn.prepareStatement("SELECT user_id, movie_id, rating FROM movie_ratings WHERE id NOT IN (SELECT id FROM " + exceptGroup + ")",
                    ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery();
            return resultsToList(rs);

        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }

        return null;
    }


    private List<Preference> resultsToList(ResultSet rs) {
        List<Preference> preferences = new ArrayList<Preference>();
        try {

            while (rs.next()) {
                long userId = rs.getLong("user_id");
                long itemId = rs.getLong("movie_id");
                float rating = rs.getFloat("rating");

                preferences.add(new GenericPreference(userId, itemId, rating));

            }

        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }

        return preferences;
    }

    /*
     * @return
     * get all users who have rated at least 76 ratings (median amount) after half of all
     * ratings have been introduced to the data model
     */
    private List<Long> getHalfOfUsers(String half) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;


        try {
            conn = MysqlDataSource.dataSource.getConnection();
            stmt = conn.prepareStatement("SELECT user_id FROM " + half + "_half_users ORDER BY user_id ASC",
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



    /*
     * print a list to a CSV file
     */
    private void printList(String s, List<String> averageList) {
        logger.info(s);
        try {
            PrintWriter writer = new PrintWriter(new FileWriter(new File("logs/"+s+".csv")));
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
            /*logger.info("DataModel without preference is: " + dataModelWithoutUser.getNumUsers());
            logger.info("DataModel with preference is: " + dataModelWithUser.getNumUsers());*/

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
            e.printStackTrace();
            logger.error("Error for user: " + userId + " on item: " + itemId + ": " + e.toString());
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
        //logger.info("Reached end of evaluateRecommenderForuser; returning NaN");

        return Double.NaN;


    }

}
