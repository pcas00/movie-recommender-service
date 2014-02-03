package edu.bc.casinepe;

import java.util.Collection;
import java.util.LinkedList;


public class MoviesBean {

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
