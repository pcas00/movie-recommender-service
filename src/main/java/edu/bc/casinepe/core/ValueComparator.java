package edu.bc.casinepe.core;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by petercasinelli on 1/28/14.
 * http://stackoverflow.com/questions/15436516/get-top-10-values-in-hash-map
 */
class ValueComparator implements Comparator<Integer> {

    Map<Integer, List<Double>> originalMap;
    //Fudge factor for confidence calculation
    public final int FUDGE_FACTOR = 2;

    public ValueComparator(Map<Integer, List<Double>> map) {
        this.originalMap = map;
    }

    public int compare(Integer keyOne, Integer keyTwo) {

        List<Double> ratingsOne = originalMap.get(keyOne);
        List<Double> ratingsTwo = originalMap.get(keyTwo);

        double avgRatingsOne = VectorOperations.mean(ratingsOne);
        double avgRatingsTwo = VectorOperations.mean(ratingsTwo);

        double valueKeyOne = avgRatingsOne - FUDGE_FACTOR / Math.sqrt(ratingsOne.size());
        double valueKeyTwo = avgRatingsTwo - FUDGE_FACTOR / Math.sqrt(ratingsTwo.size());


        if (valueKeyOne > valueKeyTwo) {
            return -1;
        //If mean's are equal, compare the amount of ratings
        } else if (avgRatingsOne == avgRatingsTwo) {

            if (ratingsOne.size() > ratingsTwo.size()) {
                return -1;
            } else {
                return 1;
            }

        } else {
            return 1;
        }
    }
}
