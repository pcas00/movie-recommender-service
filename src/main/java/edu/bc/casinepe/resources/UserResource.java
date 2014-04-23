package edu.bc.casinepe.resources;

import edu.bc.casinepe.api.MoviesBean;
import edu.bc.casinepe.jdbc.UserDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 * Created by petercasinelli on 4/23/14.
 */
@Path("user")
public class UserResource {
    private static Logger logger = LoggerFactory.getLogger(UserResource.class);

    @GET
    @Path("/{userId}/movies")
    @Produces(MediaType.APPLICATION_JSON)
    public MoviesBean getUsersMovies(@PathParam("userId") long userId) {
        logger.info("Getting users movies for user {}", userId);
        UserDAO userDAO = new UserDAO();
        return userDAO.getUsersMovies(userId);
    }

}
