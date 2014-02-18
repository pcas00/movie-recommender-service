package edu.bc.casinepe;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by petercasinelli on 1/28/14.
 */
public class VectorOperations {


    /*
     * @param ratings list of ratings from which to calculate a mean
     * @return mean rating
     */
    public static double mean(List<Double> ratings) {
        double sum = 0;
        double numberOfRatings = 0;
        for (Double rating : ratings) {
            sum += rating;
            numberOfRatings++;
        }

        return sum / numberOfRatings;
    }

    /*
     * @param n
     * @param map
     * @return a Map<Integer, List<Double>> of Top N key/values in @map based on the mean of the values in List<Double>
     */
    public static Map<Integer, List<Double>> topNFromMap(int n, Map<Integer, List<Double>> map) {
        ValueComparator vc = new ValueComparator(map);
        TreeMap<Integer, List<Double>> mapSortedByValue = new TreeMap<Integer, List<Double>>(vc);
        mapSortedByValue.putAll(map);

        //Return top 5;
        Map<Integer, List<Double>> topNMap = new LinkedHashMap<Integer, List<Double>>();
        //StringBuilder sb = new StringBuilder();

        int i = 0;
        for (Map.Entry<Integer, List<Double>> entry : mapSortedByValue.entrySet()) {
            if (i > n) {
                break;
            }
            topNMap.put(entry.getKey(), entry.getValue());
            i++;
        }

        return topNMap;
    }
}
