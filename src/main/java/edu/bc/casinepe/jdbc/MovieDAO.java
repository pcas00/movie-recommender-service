package edu.bc.casinepe.jdbc;

import com.codahale.metrics.Timer;
import edu.bc.casinepe.metrics.MetricSystem;
import edu.bc.casinepe.api.MovieBean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by petercasinelli on 2/27/14.
 */
public class MovieDAO {
    private static Logger logger = LogManager.getLogger(MovieDAO.class.getName());
    private final Timer getMovie = MetricSystem.metrics.timer(name(MovieDAO.class, "getMovie"));

    public MovieBean getMovie(int movieId) {

        final Timer.Context context = getMovie.time();
        MovieBean m = new MovieBean();
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = MysqlDataSource.getDataSource().getConnection();
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT movies.id, movies.title, AVG(movie_ratings.rating) as average_rating " +
                    "FROM movie_ratings, movies " +
                    "WHERE movies.id = movie_ratings.movie_id AND movies.id = " + movieId + " " +
                    "GROUP BY movies.id " +
                    "LIMIT 1");

            if (rs.next()) {
               m.setTitle(rs.getString("title"));
               m.setId(rs.getInt("id"));
               m.setRating(rs.getDouble("average_rating"));
            }

            rs.close();
            stmt.close();
            conn.close();


        } catch (Exception e) {
            logger.error(e);

        } finally {

            context.stop();
            return m;
        }
    }

    public static MovieBean addMovie(MovieBean movie) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = MysqlDataSource.getDataSource().getConnection();
            stmt = conn.prepareStatement("INSERT INTO movies");
            //stmt.setLong(1, userId);

            rs = stmt.executeQuery();

        } catch (SQLException e) {
            e.printStackTrace();
        }


        return new MovieBean();
    }


    public MovieBean editMovie(MovieBean movie) {
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = MysqlDataSource.getDataSource().getConnection();
            stmt = conn.prepareStatement("INSERT INTO movies");
            //stmt.setLong(1, userId);

            rs = stmt.executeQuery();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new MovieBean();
    }

    public MovieBean deleteMovie(MovieBean movie) {

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = MysqlDataSource.getDataSource().getConnection();
            stmt = conn.prepareStatement("DELETE FROM movies WHERE id=? LIMIT 1");
            stmt.setLong(1, movie.getId());

            rs = stmt.executeQuery();

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new MovieBean();
    }
}
