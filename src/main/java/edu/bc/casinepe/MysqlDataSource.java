package edu.bc.casinepe;

import org.apache.commons.dbcp.*;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.mahout.cf.taste.impl.model.jdbc.ConnectionPoolDataSource;
import org.postgresql.ds.PGPoolingDataSource;


import javax.sql.DataSource;

/**
 * Created by petercasinelli on 3/10/14.
 */
public class MysqlDataSource {

    private MysqlDataSource() {}

    public static DataSource getMysqlDataSource() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        // First, we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        GenericObjectPool.Config config = new GenericObjectPool.Config();
        config.maxActive = 150;
        config.maxIdle = 100;
        config.minIdle = 30;
        config.maxWait = 1000;

        ObjectPool connectionPool = new GenericObjectPool(null, config);

        //
        // Next, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
//		Properties p = new Properties();
//		p.setProperty("user", SQLConstants.USER_NAME);
//		p.setProperty("password", SQLConstants.PASSWORD);
//		p.setProperty("useUnicode", "true");
//		p.setProperty("characterEncoding", "UTF-8");

        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
                /*"jdbc:mysql://movie-recommender.cunjeovcml9v.us-east-1.rds.amazonaws.com:3306/movie_recommender", "pcasinelli", "alvarez2014"*/
                "jdbc:mysql://localhost/movie_recommender", "root", "");
//		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
//				connectURI, p);
        //
        // Now we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
                connectionFactory, connectionPool, null, null, false, true);

        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource poolingDataSource = new PoolingDataSource(connectionPool);

        return poolingDataSource;
    }

}
