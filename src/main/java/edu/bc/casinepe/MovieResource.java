package edu.bc.casinepe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by petercasinelli on 2/26/14.
 */
@Path("movie")
public class MovieResource {
    private String username = "petercasinelli";
    private String password = "postgres";
    private static Logger logger = LogManager.getLogger(MoviesResource.class.getName());


    @GET
    @Path("/{movieId}")
    @Produces(MediaType.APPLICATION_JSON)
    public MovieBean getMovie(@PathParam("movieId") int movieId) {
        logger.info("Retrieving movie " + movieId);
        MovieApi movieApi = new MovieApi();
        return movieApi.getMovie(movieId);
    }
}
