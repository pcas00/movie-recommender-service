package edu.bc.casinepe;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Created by petercasinelli on 1/28/14.
 * http://stackoverflow.com/questions/15436516/get-top-10-values-in-hash-map
 */
class ValueComparator implements Comparator<Integer> {

    Map<Integer, List<Double>> originalMap;

    public ValueComparator(Map<Integer, List<Double>> map) {
        this.originalMap = map;
    }

    public int compare(Integer keyOne, Integer keyTwo) {

        List<Double> ratingsOne = originalMap.get(keyOne);
        List<Double> ratingsTwo = originalMap.get(keyTwo);

        if (VectorOperations.mean(ratingsOne) > VectorOperations.mean(ratingsTwo)) {
            return -1;
        //If mean's are equal, compare the amount of ratings
        } else if (VectorOperations.mean(originalMap.get(keyOne)) == VectorOperations.mean(originalMap.get(keyTwo))) {

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
