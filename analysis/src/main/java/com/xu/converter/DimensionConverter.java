package com.xu.converter;

import com.xu.kv.DateDimension;
import com.xu.kv.UserDimension;
import com.xu.kv.base.Dimension;
import com.xu.utils.JDBCUtil;
import com.xu.utils.LRUCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//get the dimension id of user and date in mysql table
public class DimensionConverter {
    LRUCache dimensionCache = new LRUCache(500);

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

    private int sqlForDimensionID(String[] sqls) throws SQLException {
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
            String year = dateDimension.getYear();
            String month = dateDimension.getMonth();
            String day = dateDimension.getDay();

            sqls[0] = "SELECT `id` FROM `tb_date` WHERE `year` =" + Integer.valueOf(year) + "AND " +
                    "`month` =" + Integer.valueOf(month) + "AND `day` =" + Integer.valueOf(day);
            sqls[1] = "INSERT INTO `tb_date` VALUES (NULL," + Integer.valueOf(year) + "," + Integer.valueOf(month) + ","
                    + Integer.valueOf(day) + ")";
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
