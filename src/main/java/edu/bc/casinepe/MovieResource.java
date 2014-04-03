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
    private MovieDAO movieDAO = new MovieDAO();

    @GET
    @Path("/{movieId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MovieBean getMovie(@PathParam("movieId") int movieId) {
        logger.info("Retrieving movie " + movieId);
        MovieDAO movieDAO = new MovieDAO();
        return movieDAO.getMovie(movieId);
    }

    /*
     * Adds a new movie with post data
     */
    @POST
    @Path("/")
    @Produces(MediaType.APPLICATION_JSON)
    public MovieBean postMovie(MovieBean movie) {

        return movieDAO.addMovie(movie);
    }

    /*
     * Updates a movie with movie id {movieId}
     */
    @PUT
    @Path("/{movieId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MovieBean putMovie(MovieBean movie) {

       return movieDAO.editMovie(movie);
    }


   /*
    * Deletes a movie with movie id {movieId}
    */
    @DELETE
    @Path("/{movieId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MovieBean deleteMovie(MovieBean movie) {

        return movieDAO.deleteMovie(movie);
    }
}
