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

package com.datawarehouse.be.pojo.params;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.datawarehouse.be.pojo.dto.ProcessJsonDODTO;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class AirflowDAGAddParam {

    @ApiModelProperty(name = "projectName", value = "项目名称", dataType = "java.lang.Integer")
    @JsonProperty("projectName")
    private Long processId;

    @ApiModelProperty(name = "projectCode", value = "项目Code", dataType = "java.lang.String", example = "test", required = true)
    @JsonProperty("projectCode")
    private Long projectCode;

    @ApiModelProperty(name = "name", value = "工作流名称", dataType = "java.lang.String", example = "test", required = true)
    @JsonProperty("name")
    private String name;

    @JsonProperty("description")
    private String description;

    @JsonProperty("cron")
    private String cron;

    @ApiModelProperty(name = "creator", value = "创建人", dataType = "java.lang.String", example = "test", required = true)
    @JsonProperty("creator")
    private String creator;

    @JsonProperty("emails")
    private List<String> emails;

    @JsonProperty("task_count")
    private Integer taskCount;

    @JsonProperty("start_date")
    private String startDate;

    @JsonProperty("retries")
    private Integer retries;

    @JsonProperty("pool")
    private String pool;

    @JsonProperty("processJsonDO")
    private ProcessJsonDODTO processJsonDO;

//    @JsonProperty("preTasks")
//    private List<DepDO> preTasks;
//
//    @JsonProperty("outTasks")
//    private SqlParsingVO.OutPutDO outTasks;

}
