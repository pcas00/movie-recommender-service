package edu.bc.casinepe;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Collection;
import java.util.LinkedList;


public class MoviesBean {

    @JsonProperty("movies")
    Collection<MovieBean> movies;

    public MoviesBean() {
        movies = new LinkedList<MovieBean>();
    }

    public void addMovieBean(MovieBean m) {
        movies.add(m);
    }

    public Collection<MovieBean> getMovies() {
        return this.movies;
    }
}
