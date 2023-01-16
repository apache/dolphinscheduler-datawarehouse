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

package com.datawarehouse.be;

import org.apache.dolphinscheduler.api.ApiApplicationServer;
import org.apache.dolphinscheduler.api.configuration.SwaggerConfig;
import org.apache.dolphinscheduler.common.utils.PropertyUtils;
import org.apache.dolphinscheduler.dao.datasource.SpringConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.PropertySource;

import javax.annotation.PostConstruct;

import static org.apache.dolphinscheduler.common.Constants.*;

@ServletComponentScan
@ComponentScan(
        basePackages = {
            "org.apache.dolphinscheduler"
            , "com.datawarehouse.be"
        },
        excludeFilters = {
            @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = SwaggerConfig.class),
            @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = SpringConnectionFactory.class),
            @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, classes = ApiApplicationServer.class),
            @ComponentScan.Filter(type = FilterType.REGEX, pattern = {
                "org.apache.dolphinscheduler.server.*",
                "org.apache.dolphinscheduler.alert.*"
            })
        }
)
@PropertySource(ignoreResourceNotFound = false, value = "classpath:datawarehouse-api.properties")
@SpringBootApplication()
public class DatawareHouseApplicationServer extends SpringBootServletInitializer {
    public static void main(String[] args) {
        new SpringApplicationBuilder(DatawareHouseApplicationServer.class).profiles("datawarehouse-api").run(args);
    }

    @Value("${spring.datasource.driver-class-name}")
    private String driverClassName;

    @PostConstruct
    public void run() {
        PropertyUtils.setValue(SPRING_DATASOURCE_DRIVER_CLASS_NAME, driverClassName);
    }
}
