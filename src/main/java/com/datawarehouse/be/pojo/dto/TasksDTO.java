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

package com.datawarehouse.be.pojo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.apache.dolphinscheduler.common.enums.TaskTimeoutStrategy;

import java.io.*;
import java.util.Random;

@Data
public class TasksDTO<T> implements Serializable {
    @JsonProperty("code")
    private Integer code;

    @JsonProperty("name")
    private String name;

    @JsonProperty("version")
    private String version;

    @JsonProperty("description")
    private String description;

    @JsonProperty("delayTime")
    private String delayTime;

    @JsonProperty("type")
    private String taskType;

    @JsonProperty("params")
    private T taskParams;

    @JsonProperty("flag")
    private String flag;

    @JsonProperty("taskInstancePriority")
    private String taskPriority;

    @JsonProperty("workerGroup")
    private String workerGroup;

    @JsonProperty("failRetryTimes")
    private Integer failRetryTimes;

    @JsonProperty("failRetryInterval")
    private Integer failRetryInterval;

    @JsonProperty("timeoutFlag")
    private String timeoutFlag;

    @JsonProperty("timeoutNotifyStrategy")
    private TaskTimeoutStrategy timeoutNotifyStrategy;

    @JsonProperty("timeout")
    private Integer timeout;

    @JsonProperty("environmentCode")
    private Integer environmentCode;

    public void wrap(String name,String taskType){
        this.name = name;
        this.taskType = taskType;
    }

    public void wrap(String name){
        this.name = name;
    }

    public void init(){
        this.setCode(new Random().nextInt(99999 + 1));
        this.setTaskPriority("MEDIUM");
        this.setWorkerGroup("default");
        this.setFlag("YES");
        this.setFailRetryTimes(5);
        this.setFailRetryInterval(3);
        this.setTimeoutFlag("CLOSE");
        this.setTimeoutNotifyStrategy(TaskTimeoutStrategy.WARN);
        this.setTimeout(0);
        this.setDelayTime("0");
        this.setEnvironmentCode(-1);

    }

    public void wrap(String name, String taskType, Integer failRetryTimes, Integer failRetryInterval, String workerGroup) {
        this.name = name;
        this.taskType = taskType;
        this.failRetryTimes = failRetryTimes;
        this.failRetryInterval = failRetryInterval;
        this.workerGroup = workerGroup;
    }
}
