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

import com.datawarehouse.be.exceptions.ProcessParamsException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.StringUtils;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Api(value = "DepDO", tags = "依赖DO")
public class DepDO {

    /**
     * 依赖任务的项目Id
     */
    @ApiModelProperty(name = "projectCode", value = "依赖任务的项目Id", dataType = "java.lang.Integer", example = "5")
    private Integer projectId;

    /**
     * 依赖任务的工作流Id
     */
    @ApiModelProperty(name = "processId", value = "依赖任务的工作流Id", dataType = "java.lang.Integer", example = "71")
    private Long processId;
    /**
     * 工作流名称
     */
    @ApiModelProperty(name = "processName", value = "依赖任务的工作流名称", dataType = "java.lang.String", example = "测试")
    private String processName;
    /**
     * 任务名称
     */
    @ApiModelProperty(name = "taskName", value = "依赖任务的名称", dataType = "java.lang.String", example = "dwd.dwd_rent_contract_detail_f_d.$[yyyyMMdd-1].sensor")
    private String taskName;
    /**
     * 任务全称
     */
    @ApiModelProperty(name = "fullTaskName", value = "依赖任务的全称", dataType = "java.lang.String", example = "测试.sql_task")
    private String fullTaskName;
    /**
     * 表名
     */
    @ApiModelProperty(name = "tableName", value = "表名", dataType = "java.lang.String", example = "sql_task")
    private String tableName;
    /**
     * 负责人姓名
     */
    @ApiModelProperty(name = "ownerName", value = "负责人姓名", dataType = "java.lang.String", example = "weilz1")
    private String ownerName;

    /**
     * 周期
     */
    @ApiModelProperty(name = "cycle", value = "依赖的任务周期", dataType = "java.lang.String", example = "日")
    private String cycle;
    /**
     * dateValue
     */
    @ApiModelProperty(name = "dateValue", value = "依赖的任务周期值", dataType = "java.lang.String", example = "昨天")
    private String dateValue;

    public void verify() {
        if (projectId == null) {
            throw new ProcessParamsException("依赖任务的项目id[projectCode]不能为空，请确认~");
        }
        if (processId == null) {
            throw new ProcessParamsException("依赖任务id[processId]不能为空，请确认~");
        }
        if (StringUtils.isBlank(this.taskName)) {
            throw new ProcessParamsException("依赖任务名称[taskName]不能为空，请确认~！");
        }
        if (StringUtils.isBlank(this.cycle)) {
            throw new ProcessParamsException("依赖任务的周期[cycle]不能为空，请确认~！");
        }
    }
}