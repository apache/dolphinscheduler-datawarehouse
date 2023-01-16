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

package com.datawarehouse.be.pojo.dos;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.JsonObject;

import java.util.List;

public class ProcessTasksDO {
    /**
     * id
     */
    @JsonProperty("id")
    private String id;
    /**
     * name
     */
    @JsonProperty("name")
    private String name;
    /**
     * desc
     */
    @JsonProperty("desc")
    private Object desc;
    /**
     * type
     */
    @JsonProperty("type")
    private String type;
    /**
     * runFlag
     */
    @JsonProperty("runFlag")
    private String runFlag;
    /**
     * loc
     */
    @JsonProperty("loc")
    private Object loc;
    /**
     * maxRetryTimes
     */
    @JsonProperty("maxRetryTimes")
    private Integer maxRetryTimes;
    /**
     * retryInterval
     */
    @JsonProperty("retryInterval")
    private Integer retryInterval;
    /**
     * params
     */
    @JsonProperty("params")
    private JsonObject params;
    /**
     * preTasks
     */
    @JsonProperty("preTasks")
    private List<?> preTasks;
    /**
     * extras
     */
    @JsonProperty("extras")
    private Object extras;
    /**
     * depList
     */
    @JsonProperty("depList")
    private List<?> depList;
    /**
     * dependence
     */
    @JsonProperty("dependence")
    private JsonObject dependence;
    /**
     * conditionResult
     */
    @JsonProperty("conditionResult")
    private JsonObject conditionResult;
    /**
     * taskInstancePriority
     */
    @JsonProperty("taskInstancePriority")
    private String taskInstancePriority;
    /**
     * workerGroup
     */
    @JsonProperty("workerGroup")
    private String workerGroup;
    /**
     * workerGroupId
     */
    @JsonProperty("workerGroupId")
    private Object workerGroupId;
    /**
     * timeout
     */
    @JsonProperty("timeout")
    private JsonObject timeout;
    /**
     * delayTime
     */
    @JsonProperty("delayTime")
    private Integer delayTime;

}
