package edu.bc.casinepe.jdbc;

import com.codahale.metrics.Timer;
import edu.bc.casinepe.api.Rating;
import edu.bc.casinepe.metrics.MetricSystem;
import edu.bc.casinepe.api.MovieBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by petercasinelli on 2/27/14.
 */
public class MovieDAO {
    private static Logger logger = LoggerFactory.getLogger(MovieDAO.class.getName());
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
            logger.error(e.getMessage());

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

    public MovieBean putRating(Rating rating) {

        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            conn = MysqlDataSource.getDataSource().getConnection();
            stmt = conn.prepareStatement("SELECT * FROM movie_ratings WHERE user_id = ? AND movie_id = ? LIMIT 1");
            rs = stmt.executeQuery();

            // If there was already a rating for the given user and item, update it
            if (rs.next()) {
                stmt.close();
                stmt = conn.prepareStatement("UPDATE movie_ratings SET rating = ? WHERE user_id = ? AND movie_id = ? LIMIT 1");
                stmt.setFloat(1, rating.getRatingVal());
                stmt.setLong(2, rating.getUserId());
                stmt.setLong(3, rating.getMovieId());

            // No previous rating, add new one
            } else {
                stmt.close();
                stmt = conn.prepareStatement("INSERT INTO movie_ratings SET user_id = ? , movie_id = ? , rating = ?");
                stmt.setLong(1, rating.getUserId());
                stmt.setLong(2, rating.getMovieId());
                stmt.setFloat(3, rating.getRatingVal());
            }

            rs.close();
            rs = stmt.executeQuery();


        } catch (SQLException e) {
            e.printStackTrace();
        //TODO Quietly close stmt, conn, rs

        } finally {

        }

        return new MovieBean();

    }
}
