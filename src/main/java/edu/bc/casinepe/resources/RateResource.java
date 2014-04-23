package edu.bc.casinepe.resources;

import edu.bc.casinepe.api.MovieBean;
import edu.bc.casinepe.api.RatingBean;
import edu.bc.casinepe.jdbc.MovieDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("rate")
public class RateResource {

    private static Logger logger = LoggerFactory.getLogger(RateResource.class.getName());
    private MovieDAO movieDAO = new MovieDAO();

    /*
     * Updates a movie with movie id {movieId}
     */
    @PUT
    @Path("/{movieId}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public MovieBean putRating(RatingBean ratingBean) {
        logger.info("Rating was put: {}", ratingBean);
        return movieDAO.putRating(ratingBean);
    }
}
