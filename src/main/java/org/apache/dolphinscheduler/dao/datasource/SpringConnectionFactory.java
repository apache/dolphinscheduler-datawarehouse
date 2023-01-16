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

package org.apache.dolphinscheduler.dao.datasource;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.config.GlobalConfig;
import com.baomidou.mybatisplus.extension.plugins.PaginationInterceptor;
import com.baomidou.mybatisplus.extension.spring.MybatisSqlSessionFactoryBean;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.VendorDatabaseIdProvider;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.type.JdbcType;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import java.util.Properties;

@Configuration
@MapperScan(
        basePackages = "org.apache.dolphinscheduler.*.mapper",
        sqlSessionFactoryRef = "sqlSessionFactory"
)
public class SpringConnectionFactory {

    @Value("${spring.datasource.driver-class-name}")
    private String driverClassName;

    @Value("${spring.datasource.url}")
    private String jdbcUrl;

    @Value("${spring.datasource.username}")
    private String username;

    @Value("${spring.datasource.password}")
    private String password;

    @Value("${spring.datasource.hikari.connection-test-query}")
    private String testQuery;

    @Value("${spring.datasource.hikari.minimum-idle}")
    private int minimumIdle;

    @Value("${spring.datasource.hikari.auto-commit}")
    private boolean autoCommit;

    @Value("${spring.datasource.hikari.validation-timeout}")
    private int validationTimeout;

    @Value("${spring.datasource.hikari.pool-name}")
    private String poolName;

    @Value("${spring.datasource.hikari.maximum-pool-size}")
    private int maximumPoolSize;

    @Value("${spring.datasource.hikari.connection-timeout}")
    private long connectionTimeout;

    @Value("${spring.datasource.hikari.idle-timeout}")
    private long idleTimeout;

    @Value("${spring.datasource.hikari.leak-detection-threshold}")
    private long leakDetectionThreshold;

    @Value("${spring.datasource.hikari.initialization-fail-timeout}")
    private long initializationFailTimeout;

    /**
     *  pagination interceptor
     * @return pagination interceptor
     */
    @Bean
    public PaginationInterceptor paginationInterceptor() {
        return new PaginationInterceptor();
    }

    /**
     * get the data source
     * @return druid dataSource
     */
    @Bean(destroyMethod="")
    @Primary
    public HikariDataSource dataSource() {

        HikariDataSource dataSource = new HikariDataSource();

        dataSource.setDriverClassName(driverClassName);
        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        dataSource.setConnectionTestQuery(testQuery);
        dataSource.setMinimumIdle(minimumIdle);
        dataSource.setAutoCommit(autoCommit);
        dataSource.setValidationTimeout(validationTimeout);
        dataSource.setPoolName(poolName);
        dataSource.setMaximumPoolSize(maximumPoolSize);
        dataSource.setConnectionTimeout(connectionTimeout);
        dataSource.setIdleTimeout(idleTimeout);
        dataSource.setLeakDetectionThreshold(leakDetectionThreshold);
        dataSource.setInitializationFailTimeout(initializationFailTimeout);

        return dataSource;
    }

    /**
     * * get transaction manager
     * @return DataSourceTransactionManager
     */
    @Bean
    @Primary
    public DataSourceTransactionManager transactionManager() {
        return new DataSourceTransactionManager(dataSource());
    }

    /**
     * * get sql session factory
     * @return sqlSessionFactory
     * @throws Exception sqlSessionFactory exception
     */

    @Primary
    @Bean
    public SqlSessionFactory sqlSessionFactory() throws Exception {
        MybatisConfiguration configuration = new MybatisConfiguration();
        configuration.setMapUnderscoreToCamelCase(true);
        configuration.setCacheEnabled(false);
        configuration.setCallSettersOnNulls(true);
        configuration.setJdbcTypeForNull(JdbcType.NULL);
        configuration.addInterceptor(paginationInterceptor());

        configuration.setGlobalConfig(new GlobalConfig().setBanner(false));
        MybatisSqlSessionFactoryBean sqlSessionFactoryBean = new MybatisSqlSessionFactoryBean();
        sqlSessionFactoryBean.setConfiguration(configuration);
        sqlSessionFactoryBean.setDataSource(dataSource());

        GlobalConfig.DbConfig dbConfig = new GlobalConfig.DbConfig();
        dbConfig.setIdType(IdType.AUTO);
        GlobalConfig globalConfig = new GlobalConfig();
        globalConfig.setDbConfig(dbConfig);
        sqlSessionFactoryBean.setGlobalConfig(globalConfig);
        sqlSessionFactoryBean.setTypeAliasesPackage("org.apache.dolphinscheduler.dao.entity");
        ResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        sqlSessionFactoryBean.setMapperLocations(resolver.getResources("org/apache/dolphinscheduler/dao/mapper/*Mapper.xml"));
        sqlSessionFactoryBean.setTypeEnumsPackage("org.apache.dolphinscheduler.*.enums");
        sqlSessionFactoryBean.setDatabaseIdProvider(databaseIdProvider());
        return sqlSessionFactoryBean.getObject();
    }

    /**
     * get sql session
     * @return SqlSession
     * @throws Exception
     */
    @Bean
    @Primary
    public SqlSession sqlSession() throws Exception{
        return new SqlSessionTemplate(sqlSessionFactory());
    }

    @Bean
    public DatabaseIdProvider databaseIdProvider() {
        DatabaseIdProvider databaseIdProvider = new VendorDatabaseIdProvider();
        Properties properties = new Properties();
        properties.setProperty("MySQL", "mysql");
//        properties.setProperty("PostgreSQL", "pg");
//        properties.setProperty("h2", "h2");
        databaseIdProvider.setProperties(properties);
        return databaseIdProvider;
    }
}
