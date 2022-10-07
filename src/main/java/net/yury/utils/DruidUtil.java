package net.yury.utils;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

public class DruidUtil {
    private static final DruidDataSource druidDataSource = fromConfigFile("pg-druid.properties");

    public static DruidDataSource fromConfigFile(String configFile) {
        try {
            InputStream is = DruidUtil.class.getClassLoader().getResourceAsStream(configFile);
            Properties properties = new Properties();
            assert is != null;
            properties.load(is);
            return (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
        }catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static DruidPooledConnection getConnection() throws SQLException {
        return druidDataSource.getConnection();
    }

    public static void shutdown() {
        druidDataSource.close();
    }
}
