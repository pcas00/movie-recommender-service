package edu.bc.casinepe.jdbc;

import com.codahale.metrics.Timer;
import edu.bc.casinepe.api.RatingBean;
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

    public MovieBean getMovie(long movieId) {

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

    public MovieBean updateRating(RatingBean rating) {
        logger.info("updateRating for user {} on item {} with rating {}", rating.getUserId(), rating.getMovieId(), rating.getRating());
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int rowsAffected = 0;

        try {
            conn = MysqlDataSource.getDataSource().getConnection();

            stmt = conn.prepareStatement("UPDATE movie_ratings SET rating = ?, timestamp = ? WHERE user_id = ? AND movie_id = ? LIMIT 1");
            stmt.setFloat(1, rating.getRating());
            stmt.setInt(2, (int) (System.currentTimeMillis() / 1000L));
            stmt.setLong(3, rating.getUserId());
            stmt.setLong(4, rating.getMovieId());
            rowsAffected = stmt.executeUpdate();
            conn.commit();

            logger.info("Rating found; updating {} rows", rowsAffected);

        } catch (SQLException e) {
            logger.error("SQL Exception: {}", e);
            e.printStackTrace();
        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }

        MovieBean m = getMovie(rating.getMovieId());
        return new MovieBean(rating.getMovieId(), m.getTitle(), rating.getRating());
    }

    public MovieBean addRating(RatingBean rating) {

        logger.info("addRating for user {} on item {} with rating {}", rating.getUserId(), rating.getMovieId(), rating.getRating());
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        int rowsAffected = 0;

        try {
            conn = MysqlDataSource.getDataSource().getConnection();

            stmt = conn.prepareStatement("INSERT INTO movie_ratings (user_id, movie_id, rating, timestamp) VALUES (?, ?, ?, ?)");
            stmt.setLong(1, rating.getUserId());
            stmt.setLong(2, rating.getMovieId());
            stmt.setFloat(3, rating.getRating());
            stmt.setInt(4, (int) (System.currentTimeMillis() / 1000L));
            rowsAffected = stmt.executeUpdate();
            conn.commit();
            logger.info("No rating found; inserting {} rows", rowsAffected);

        } catch (SQLException e) {
            logger.error("SQL Exception: {}", e);
            e.printStackTrace();
        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }

        MovieBean m = getMovie(rating.getMovieId());
        return new MovieBean(rating.getMovieId(), m.getTitle(), rating.getRating());

    }

    public MovieBean putRating(RatingBean ratingBean) {
        logger.info("putRating for user {} on item {} with rating {}", ratingBean.getUserId(), ratingBean.getMovieId(), ratingBean.getRating());
        Connection conn = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        PreparedStatement stmtTwo = null;
        int rowsAffected = 0;
        try {
            conn = MysqlDataSource.getDataSource().getConnection();
            stmt = conn.prepareStatement("SELECT * FROM movie_ratings WHERE user_id = ? AND movie_id = ? LIMIT 1");
            stmt.setLong(1, ratingBean.getUserId());
            stmt.setLong(2, ratingBean.getMovieId());
            rs = stmt.executeQuery();
            // If there was already a ratingBean for the given user and item, update it
            if (rs.next()) {
                return this.updateRating(ratingBean);
             // No previous ratingBean, add new one
            } else {
                return this.addRating(ratingBean);
            }

        } catch (SQLException e) {
            logger.error("SQL Exception: {}", e);
        } finally {
            org.apache.mahout.common.IOUtils.quietClose(rs, stmt, conn);
        }
        return null;

    }
}
