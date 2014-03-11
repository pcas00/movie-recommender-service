package edu.bc.casinepe;

import com.codahale.metrics.Timer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by petercasinelli on 2/27/14.
 */
public class MovieApi {
    private static Logger logger = LogManager.getLogger(MovieApi.class.getName());
    private final Timer getMovie = MetricSystem.metrics.timer(name(MovieApi.class, "getMovie"));

    public MovieBean getMovie(int movieId) {

        final Timer.Context context = getMovie.time();
        MovieBean m = new MovieBean();
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = PGDataSource.getDataSource().getConnection();
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT movies.id, movies.title, AVG(movie_ratings.rating) as average_rating " +
                    "FROM movie_ratings, movies " +
                    "WHERE movies.id = movie_ratings.movie_id AND movies.id = " + movieId +
                    "GROUP BY movies.id " +
                    "LIMIT 1");

            if (rs.next()) {
               m.setTitle(rs.getString("title"));
               m.setId(rs.getInt("id"));
               m.setRating(rs.getDouble("average_rating"));
            }


        } catch (SQLException e) {
            logger.error(e);

        } finally {

            context.stop();
            return m;
        }
    }

    /*public MovieBean addMovie() {

    }

    public MovieBean editMovie(int movieId) {

    }

    public MovieBean deleteMovie(int movieId) {

    }*/
}
