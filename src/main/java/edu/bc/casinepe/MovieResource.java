package edu.bc.casinepe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by petercasinelli on 2/26/14.
 */
@Path("movie")
public class MovieResource {
    private static Logger logger = LogManager.getLogger(MoviesResource.class.getName());


    @GET
    @Path("/{movieId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MovieBean getMovie(@PathParam("movieId") int movieId) {
        logger.info("Retrieving movie " + movieId);
        MovieApi movieApi = new MovieApi();
        return movieApi.getMovie(movieId);
    }

    /*
     * Adds a new movie with post data
     */
    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public MovieBean postMovie() {

        return new MovieBean();
    }

    /*
     * Updates a movie with movie id {movieId}
     */
    @PUT
    @Path("/{movieId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MovieBean putMovie(@PathParam("movieId") int movieId) {

       return new MovieBean();
    }


   /*
    * Deletes a movie with movie id {movieId}
    */
    @DELETE
    @Path("/{movieId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MovieBean deleteMovie(@PathParam("movieId") int movieId) {

        return new MovieBean();
    }
}
