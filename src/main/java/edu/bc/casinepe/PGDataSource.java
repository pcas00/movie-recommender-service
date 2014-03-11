package edu.bc.casinepe;

import org.postgresql.ds.PGPoolingDataSource;

/**
 * Created by petercasinelli on 3/10/14.
 */
public class PGDataSource {

    private static PGPoolingDataSource dataSource = null;

    private PGDataSource() {}

    public static PGPoolingDataSource getDataSource() {
        if (dataSource == null) {
            dataSource = new PGPoolingDataSource();
            dataSource.setDataSourceName("movie-recommender-ds");
            dataSource.setServerName("localhost");
            dataSource.setDatabaseName("movie_recommender");
            dataSource.setUser("petercasinelli");
            dataSource.setPassword("password");
            dataSource.setMaxConnections(10);
        }
        return dataSource;
    }
}
