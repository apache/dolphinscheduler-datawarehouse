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
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;

import java.io.Serializable;

@Api(value = "DependItemListDTO", tags = "依赖项集合")
@Data
public class DependItemListDTO implements Serializable {

    @ApiModelProperty(name = "projectCode", value = "项目Id,业务线DAG迁移前约定好值", dataType = "java.lang.Integer")
    @JsonProperty("projectCode")
    private Integer projectCode;

    @ApiModelProperty(name = "definitionCode", value = "工作流Id,业务线DAG迁移前约定好值", dataType = "java.lang.Integer")
    @JsonProperty("definitionCode")
    private Long definitionCode;

    @ApiModelProperty(name = "depTasks", value = "0对ALL", dataType = "java.lang.String", required = true)
    @JsonProperty("depTasks")
    private Integer depTaskCode;

    @ApiModelProperty(name = "cycle", value = "依赖的任务周期,DS后台来设置", dataType = "java.lang.String", hidden = true)
    @JsonProperty("cycle")
    private String cycle;

    @ApiModelProperty(name = "dateValue", value = "依赖的任务周期值,DS后台来设置", dataType = "java.lang.String", hidden = true)
    @JsonProperty("dateValue")
    private String dateValue;

    @JsonProperty("status")
    private ExecutionStatus status;

    public DependItemListDTO(Integer projectCode, Long definitionCode, Integer depTaskCode, String cycle, String dateValue) {
        this.projectCode = projectCode;
        this.definitionCode = definitionCode;
        this.depTaskCode = depTaskCode;
        this.cycle = cycle;
        this.dateValue = dateValue;
    }
}