package edu.bc.casinepe;

import java.util.List;

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
}
