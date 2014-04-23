package edu.bc.casinepe.jdbc;

import com.codahale.metrics.Timer;
import edu.bc.casinepe.api.MovieBean;
import edu.bc.casinepe.api.MoviesBean;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by petercasinelli on 4/23/14.
 */
public class UserDAO {
    private static Logger logger = LoggerFactory.getLogger(UserDAO.class);

    public MoviesBean getUsersMovies(long userId) {
        //final Timer.Context context = getMovie.time();
        logger.info("UserDAO attempting to retrieve movie ratings for user {}", userId);
        MoviesBean movies = new MoviesBean();
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            conn = MysqlDataSource.getDataSource().getConnection();
            stmt = conn.prepareStatement("SELECT movies.title AS title, movie_id, rating FROM movie_ratings INNER JOIN movies ON movie_ratings.movie_id = movies.id WHERE user_id = ? LIMIT 5");
            stmt.setLong(1, userId);
            rs = stmt.executeQuery();

            while (rs.next()) {
                MovieBean m = new MovieBean();
                m.setTitle(rs.getString("title"));
                m.setId(rs.getInt("movie_id"));
                m.setRating(rs.getDouble("rating"));
                movies.addMovieBean(m);
            }

        } catch (Exception e) {
            logger.error(e.getMessage());

        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
            //context.stop();
        }
        logger.info("Movies are: " + movies.getMovies());
        return movies;

    }

}
