package com.xu.converter;

import com.xu.kv.DateDimension;
import com.xu.utils.JDBCUtil;
import com.xu.utils.LRUCache;
import com.xu.kv.UserDimension;
import com.xu.kv.base.Dimension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//get the dimension id of user and date in mysql table
public class DimensionConverter {
    private LRUCache dimensionCache = new LRUCache(500);

    public int getDimensionID(Dimension dimension) throws SQLException {
        String dimensionKey = getDimensionKey(dimension);
        //get the dimension ID in the cache
        if (dimensionCache.containsKey(dimensionKey)) {
            return dimensionCache.get(dimensionKey);
        }

        //get sqls
        String[] sqls = getSqls(dimension);

        //use sql to get dimension ID
        int dimensionID = sqlForDimensionID(sqls);
        if (dimensionID == 0) throw new RuntimeException("can not find dimension id !");

        //write dimension information to the cache
        dimensionCache.put(dimensionKey, dimensionID);
        //return the dimension ID
        return dimensionID;
    }

    private synchronized int sqlForDimensionID(String[] sqls) throws SQLException {
        Connection connection = JDBCUtil.getInstance();
        //query whether there is a value for this dimension in the database
        PreparedStatement preparedStatement = connection.prepareStatement(sqls[0]);
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        //if not, insert dimension data
        preparedStatement = connection.prepareStatement(sqls[1]);
        preparedStatement.executeUpdate();
        //execute the query again
        preparedStatement = connection.prepareStatement(sqls[0]);
        resultSet = preparedStatement.executeQuery();
        if (resultSet.next()) {
            return resultSet.getInt(1);
        }
        return 0;
    }


    private String[] getSqls(Dimension dimension) {
        String[] sqls = new String[2];
        if (dimension instanceof UserDimension) {
            UserDimension userDimension = (UserDimension) dimension;
            String phoneNo = userDimension.getPhoneNo();
            String userName = userDimension.getUserName();

            sqls[0] = "SELECT `id` FROM `tb_users` WHERE `phone` =" + "'" + phoneNo + "'";
            sqls[1] = "INSERT INTO `tb_users` VALUES (NULL," + "'" + phoneNo + "','" + userName + "')";
        } else {
            DateDimension dateDimension = (DateDimension) dimension;
            int call_year = Integer.valueOf(dateDimension.getYear());
            int call_month = Integer.valueOf(dateDimension.getMonth());
            int call_day = Integer.valueOf(dateDimension.getDay());

            sqls[0] = "SELECT `id` FROM `tb_date` WHERE `year` =" + call_year + "AND " +
                    "`month` =" + call_month + "AND `day` =" + call_day;
            sqls[1] = "INSERT INTO `tb_date` VALUES (NULL," + call_year + "," + call_month + ","
                    + call_day + ")";
        }
        return sqls;
    }

    private String getDimensionKey(Dimension dimension) {
        String dimensionKey;
        if (dimension instanceof UserDimension) {
            UserDimension userDimension = (UserDimension) dimension;
            dimensionKey = userDimension.getPhoneNo();
        } else {
            DateDimension dateDimension = (DateDimension) dimension;
            dimensionKey = dateDimension.getYear() + dateDimension.getMonth() + dateDimension.getDay();
        }
        return dimensionKey;
    }
}
