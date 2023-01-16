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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Configuration
@Component
public class SensorConfig {
    private static final Logger logger = LoggerFactory.getLogger(SensorConfig.class);

    /**
     * 分区探测所属项目ID
     */
    @Value("${sensor.project.id}")
    public int projectId;
    /**
     * 分区探测所属项目ID
     */
    @Value("${sensor.project.name}")
    public String projectName;

    /**
     * 分区探测所属工作流ID
     */
    @Value("${sensor.process.id}")
    public Long processId;
    /**
     * 分区探测所属工作流名称
     */
    @Value("${sensor.process.name}")
    public String processName;
    /**
     * 分区探测项目所属用户
     */
    @Value("${sensor.project.owner}")
    public String projectOwner;

    @PostConstruct
    public void init() {
        logger.info("SensorConfig:projectCode,projectName,processId,processName,processOwner:{},{},{},{},{}", projectId, projectName, processId, processName,projectOwner);
    }
}
