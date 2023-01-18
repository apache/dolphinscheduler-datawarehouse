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

package com.datawarehouse.be.configuration;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import javax.sql.DataSource;

@Configuration
@MapperScan(
        basePackages = "com.datawarehouse.be.dao.hivemeta",
        sqlSessionFactoryRef = "hiveMetaSqlSessionFactory",
        sqlSessionTemplateRef = "hiveMetaSqlSessionTemplate"
)
public class HiveMetaConnectionConfig {

    @Autowired
    @Qualifier("hiveMetaDataSource")
    private DataSource hiveMetaDataSource;

    public HiveMetaConnectionConfig() {
    }

    @Bean(name= "hiveMetaTransactionManager")
    public DataSourceTransactionManager hiveTabMetaTransactionManager() {
        return new DataSourceTransactionManager(hiveMetaDataSource);
    }


    @Bean(name = "hiveMetaSqlSessionFactory")
    public SqlSessionFactory hiveTabMetaSqlSessionFactory() throws Exception {
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setDataSource(hiveMetaDataSource);
        return bean.getObject();
    }

    @Bean("hiveMetaSqlSessionTemplate")
    public SqlSessionTemplate hiveMetaSqlSessionTemplate() throws Exception {
        return new SqlSessionTemplate(hiveTabMetaSqlSessionFactory());
    }
}
