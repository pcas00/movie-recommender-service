package edu.bc.casinepe.api;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by petercasinelli on 4/11/14.
 */
public class RatingBean {

    @JsonProperty("userId")
    long userId;
    @JsonProperty("movieId")
    long movieId;
    @JsonProperty("rating")
    float rating;

    public RatingBean() {}

    public RatingBean(long userId, long movieId, float rating) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
    }

    public float getRating() {
        return rating;
    }

    public long getUserId() {
        return userId;
    }

    public long getMovieId() {
        return movieId;
    }


}
