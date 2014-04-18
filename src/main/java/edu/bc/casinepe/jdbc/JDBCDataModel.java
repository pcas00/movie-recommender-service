package edu.bc.casinepe.jdbc;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.jdbc.MySQLJDBCDataModel;
import org.apache.mahout.cf.taste.impl.model.jdbc.ReloadFromJDBCDataModel;
import org.apache.mahout.cf.taste.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by petercasinelli on 4/11/14.
 */
public class JDBCDataModel {
    private static Logger logger = LoggerFactory.getLogger(JDBCDataModel.class.getName());
    private static DataModel dataModel;

    static {
        try {
            dataModel = new ReloadFromJDBCDataModel(new MySQLJDBCDataModel(MysqlDataSource.getDataSource(),
                    "movie_ratings",
                    "user_id",
                    "movie_id",
                    "rating",
                    "timestamp"));
        } catch (TasteException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    private JDBCDataModel() {}

    public static DataModel getDataModel() {
        return dataModel;
    }
}
