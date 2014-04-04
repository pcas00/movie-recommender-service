package edu.bc.casinepe.core;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.Refreshable;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.*;
import org.apache.mahout.cf.taste.impl.recommender.AbstractRecommender;
import org.apache.mahout.cf.taste.impl.recommender.TopItems;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.IDRescorer;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by petercasinelli on 4/2/14.
 */
public class ConfidenceItemAverageRecommender extends AbstractRecommender {

    private static org.apache.logging.log4j.Logger logger = LogManager.getLogger(ConfidenceItemAverageRecommender.class);

    private final FastByIDMap<RunningAverage> itemAverages;
    private final ReadWriteLock buildAveragesLock;
    private final RefreshHelper refreshHelper;

    public ConfidenceItemAverageRecommender(DataModel dataModel) throws TasteException {
        super(dataModel);
        this.itemAverages = new FastByIDMap<RunningAverage>();
        this.buildAveragesLock = new ReentrantReadWriteLock();
        this.refreshHelper = new RefreshHelper(new Callable<Object>() {
            @Override
            public Object call() throws TasteException {
                buildAverageDiffs();
                return null;
            }
        });
        refreshHelper.addDependency(dataModel);
        buildAverageDiffs();
    }

    @Override
    public List<RecommendedItem> recommend(long userID, int howMany, IDRescorer rescorer) throws TasteException {
        Preconditions.checkArgument(howMany >= 1, "howMany must be at least 1");
        logger.debug("Recommending items for user ID '{}'", userID);

        PreferenceArray preferencesFromUser = getDataModel().getPreferencesFromUser(userID);
        FastIDSet possibleItemIDs = getAllOtherItems(userID, preferencesFromUser);

        TopItems.Estimator<Long> estimator = new Estimator();

        IDRescorer customIdRescorer = new IDRescorer() {
            @Override
            public double rescore(long id, double originalScore) {
                double newScore = originalScore;
                try {
                    int numberOfRatings = getDataModel().getNumUsersWithPreferenceFor(id);
                    newScore = originalScore - ConfidenceItemUserAverageRecommender.RESCORE_CONSTANT_VALUE / Math.sqrt(numberOfRatings);
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

        MultipleIDRescorer multipleIDRescorer = new MultipleIDRescorer(rescorer);
        multipleIDRescorer.addIdRescorer(customIdRescorer);

        List<RecommendedItem> topItems = TopItems.getTopItems(howMany, possibleItemIDs.iterator(), multipleIDRescorer,
                estimator);

        logger.debug("Recommendations are: {}", topItems);
        return topItems;
    }

    @Override
    public float estimatePreference(long userID, long itemID) throws TasteException {
        DataModel dataModel = getDataModel();
        Float actualPref = dataModel.getPreferenceValue(userID, itemID);
        if (actualPref != null) {
            return actualPref;
        }
        return doEstimatePreference(itemID);
    }

    private float doEstimatePreference(long itemID) {
        buildAveragesLock.readLock().lock();
        try {
            RunningAverage average = itemAverages.get(itemID);
            int numberOfRatings = getDataModel().getNumUsersWithPreferenceFor(itemID);

            return average == null ? Float.NaN
                                   : (float) (average.getAverage() -
                                              ConfidenceItemUserAverageRecommender.RESCORE_CONSTANT_VALUE
                                              / Math.sqrt(numberOfRatings));
        } catch (TasteException e) {
            logger.error(e.getMessage());
            return 0f;
        } finally {
            buildAveragesLock.readLock().unlock();
        }
    }

    private void buildAverageDiffs() throws TasteException {
        try {
            buildAveragesLock.writeLock().lock();
            DataModel dataModel = getDataModel();
            LongPrimitiveIterator it = dataModel.getUserIDs();
            while (it.hasNext()) {
                PreferenceArray prefs = dataModel.getPreferencesFromUser(it.nextLong());
                int size = prefs.length();
                for (int i = 0; i < size; i++) {
                    long itemID = prefs.getItemID(i);
                    RunningAverage average = itemAverages.get(itemID);
                    if (average == null) {
                        average = new FullRunningAverage();
                        itemAverages.put(itemID, average);
                    }
                    average.addDatum(prefs.getValue(i));
                }
            }
        } finally {
            buildAveragesLock.writeLock().unlock();
        }
    }

    @Override
    public void setPreference(long userID, long itemID, float value) throws TasteException {
        DataModel dataModel = getDataModel();
        double prefDelta;
        try {
            Float oldPref = dataModel.getPreferenceValue(userID, itemID);
            prefDelta = oldPref == null ? value : value - oldPref;
        } catch (NoSuchUserException nsee) {
            prefDelta = value;
        }
        super.setPreference(userID, itemID, value);
        try {
            buildAveragesLock.writeLock().lock();
            RunningAverage average = itemAverages.get(itemID);
            if (average == null) {
                RunningAverage newAverage = new FullRunningAverage();
                newAverage.addDatum(prefDelta);
                itemAverages.put(itemID, newAverage);
            } else {
                average.changeDatum(prefDelta);
            }
        } finally {
            buildAveragesLock.writeLock().unlock();
        }
    }

    @Override
    public void removePreference(long userID, long itemID) throws TasteException {
        DataModel dataModel = getDataModel();
        Float oldPref = dataModel.getPreferenceValue(userID, itemID);
        super.removePreference(userID, itemID);
        if (oldPref != null) {
            try {
                buildAveragesLock.writeLock().lock();
                RunningAverage average = itemAverages.get(itemID);
                if (average == null) {
                    throw new IllegalStateException("No preferences exist for item ID: " + itemID);
                } else {
                    average.removeDatum(oldPref);
                }
            } finally {
                buildAveragesLock.writeLock().unlock();
            }
        }
    }

    @Override
    public void refresh(Collection<Refreshable> alreadyRefreshed) {
        refreshHelper.refresh(alreadyRefreshed);
    }

    @Override
    public String toString() {
        return "ItemAverageRecommender";
    }

    private final class Estimator implements TopItems.Estimator<Long> {

        @Override
        public double estimate(Long itemID) {
            return doEstimatePreference(itemID);
        }
    }
}
