package edu.bc.casinepe.api;

/**
 * Created by petercasinelli on 4/11/14.
 */
public class Rating {

    long userId;
    long movieId;
    float ratingVal;

    public Rating(long userId, long movieId, float ratingVal) {
        this.userId = userId;
        this.movieId = movieId;
        this.ratingVal = ratingVal;
    }

    public float getRatingVal() {
        return ratingVal;
    }

    public long getUserId() {
        return userId;
    }

    public long getMovieId() {
        return movieId;
    }


}
