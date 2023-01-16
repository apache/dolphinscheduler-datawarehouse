/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datawarehouse.be.dao.impl;

import com.datawarehouse.be.dao.InputTableDAO;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Slf4j
@Component
public class InputTableDAOImpl implements InputTableDAO {

    /**
     * 初始话JDBCDriver
     */
    private static String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    // 本地测试使用
    @Value("${hive.permission.connect.url:jdbc:hive2://10.216.138.26:10000/}")
    private String CONNECTION_URL;
//    private static String CONNECTION_URL = "jdbc:hive2://10.216.138.23:10500/";
    // 测试、生产环境使用
//    private static String CONNECTION_URL = "jdbc:hive2://zrhadoopmaster1:2181,zrhadoopmaster2:2181,zrhadoopslave1:2181/;serviceDiscoveryMode=zooKeeperHA;zooKeeperNamespace=hs2ActivePassiveHA";
//    private static String CONNECTION_URL = "jdbc:hive2://hadoopmaster-03.bi.datawarehouse.com:2181,zrhadoopmaster1:2181,zrhadoopmaster2:2181,zrhadoopslave1:2181,hadoopclient-06.bi.datawarehouse.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2";

    //    private static String username = "data-extraction";
//    private static String password = "ayJJyxZUkGJh1hjq";
//
    static {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (Exception e) {
            log.error("初始化连接Hive的JDBCDriver失败，详情：{}", e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * 是否拥有表权限
     * @param userName 用户名
     * @param sql sql
     * @return
     * @throws SQLException
     */
    @Override
    public boolean isHavingPermissionOfTable(String userName, String sql) throws SQLException {
        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        String password = "r7cEvVBURr4TcM0C";

        try {
            // 拼接sql，绑定入参
            connection = DriverManager.getConnection(new StringBuffer(CONNECTION_URL).append(";hive.server2.proxy.user=").append(userName).toString(), "hue", password);
//            connection = DriverManager.getConnection(new StringBuffer(CONNECTION_URL).append(";hive.server2.proxy.user=").append("echo").toString(), "hue", password);
            ps = connection.prepareStatement(sql);
            // 获取数据
            rs = ps.executeQuery();
            // 执行无异常，则表示有表查询权限，反之则无权限
            return true;
        } catch (Exception e) {
            log.error("调用isHavingPermissionOfTable查询失败，详情：{}", e.getMessage());
            throw e;
        } finally {
            // 关闭流
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                log.error("关闭isHavingPermissionOfTable方法中中流失败，详情：{}", e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * 是否拥有表权限
     * @param userName 用户名
     * @param sql 待校验的sql
     * @param envSqlList 设置的环境变量集合
     * @return
     * @throws SQLException
     */
    @Override
    public boolean isHavingPermissionOfTable(String userName, String sql, List<String> envSqlList) throws SQLException {

        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        String password = "r7cEvVBURr4TcM0C";

        try {
            // 拼接sql，绑定入参
            connection = DriverManager.getConnection(new StringBuffer(CONNECTION_URL).append(";hive.server2.proxy.user=").append(userName).toString(), "hue", password);

            ps = connection.prepareStatement(sql);
            if (CollectionUtils.isNotEmpty(envSqlList)) {
                for (String envSql : envSqlList) {
                    // hive sql中set变量执行结果为false属于正常，不影响结果（返回boolean，true表示查询语句，false表示增删改）
                    ps.execute(envSql);
                }
            }
            // 获取数据
            rs = ps.executeQuery();
            // 执行无异常，则表示有表查询权限，反之则无权限
            return true;
        } catch (Exception e) {
            log.error("调用isHavingPermissionOfTable查询失败，详情：{}", e.getMessage());
            throw e;
        } finally {
            // 关闭流
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                log.error("关闭isHavingPermissionOfTable方法中中流失败，详情：{}", e.getMessage());
                e.printStackTrace();
            }
        }
    }

    /**
     * 是否拥有表权限
     * @param userName 用户名
     * @param sql 待校验的sql
     * @param envSql 设置的环境变量
     * @return
     * @throws SQLException
     */
    @Override
    public boolean isHavingPermissionOfTable(String userName, String sql, String envSql) throws SQLException {

        Connection connection = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        String password = "r7cEvVBURr4TcM0C";

        try {
            // 拼接sql，绑定入参
            connection = DriverManager.getConnection(new StringBuffer(CONNECTION_URL).append(";hive.server2.proxy.user=").append(userName).toString(), "hue", password);
            ps = connection.prepareStatement(sql);
            if (StringUtils.isNotBlank(envSql)) {
                // hive sql中set变量执行结果为false属于正常，不影响结果
                ps.execute(envSql);
            }
            // 获取数据
            rs = ps.executeQuery();
            // 执行无异常，则表示有表查询权限，反之则无权限
            return true;
        } catch (Exception e) {
            log.error("调用isHavingPermissionOfTable查询失败，详情：{}", e.getMessage());
            throw e;
        } finally {
            // 关闭流
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e) {
                log.error("关闭isHavingPermissionOfTable方法中中流失败，详情：{}", e.getMessage());
                e.printStackTrace();
            }
        }

    }
}
