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

package com.datawarehouse.be.pojo.vo;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.datawarehouse.be.pojo.dos.CustomWarningDO;
import com.datawarehouse.be.pojo.dto.QueryDto;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Api(tags = "ProcessDetailVO")
public class ProcessDetailVO {

    /**
     * 用户自定义告警信息
     */
    @ApiModelProperty(name = "customWarningParam", value = "用户定义告警信息", dataType = "com.datawarehouse.be.pojo.dos.CustomWarningParam")
    private CustomWarningDO customWarningDO;


    /**
     * sql 拼接请求参数
     */
    @ApiModelProperty(name = "queryDto", value = "sql拼接请求参数", dataType = "com.datawarehouse.be.pojo.dto.QueryDto")
    private QueryDto queryDto;

    /**
     * process Id
     */
    @ApiModelProperty(name = "processId", value = "工作流Id", dataType = "java.lang.Integer", example = "1")
    private Long processId;

    /**
     * process Id
     */
    @ApiModelProperty(name = "sql", value = "工作流SQL", dataType = "java.lang.String", example = "")
    private String sql;

    /**
     * name
     */
    @ApiModelProperty(name = "name", value = "工作流名称", dataType = "java.lang.String", example = "test", required = true)
    @JsonProperty("name")
    private String name;
    /**
     * description
     */
    @ApiModelProperty(name = "description", value = "工作流描述", dataType = "java.lang.String", example = "测试工作流", required = true)
    @JsonProperty("description")
    private String description;

    /**
     * 周期
     */
    @ApiModelProperty(name = "cycle", value = "工作流周期", dataType = "java.lang.String", example = "每日")
    private String cycle;

    /**
     * dateValue
     */
    @ApiModelProperty(name = "dateValue", value = "工作流周期值", dataType = "java.lang.String", example = "")
    private String dateValue;

    /**
     * timeValue
     */
    @ApiModelProperty(name = "timeValue", value = "工作流周期时间值", dataType = "java.lang.String", example = "02:00")
    private String timeValue;

    /**
     * 工作流依赖集合
     */
    @ApiModelProperty(name = "depDOList", value = "工作流依赖集合", dataType = "java.lang.List<DepDO>", example = "")
    private List<DepDO> depDOList;

    /**
     * 工作流的产出（表）
     */
    @ApiModelProperty(name = "outPutDO", value = "工作流产出集合", dataType = "java.lang.List<ExportDO>", example = "")
    private SqlParsingVO.OutPutDO outPutDO;

    /**
     * 工作流中导出
     */
    @ApiModelProperty(name = "exportDO", value = "工作流产出集合", dataType = "java.lang.List<ExportDO>", example = "")
    private SqlParsingVO.ExportDO exportDO;

    @ApiModelProperty(name = "projectCode", value = "项目Id", dataType = "java.lang.Integer", example = "1")
    private long projectId;

    @ApiModelProperty(name = "projectName", value = "项目名称", dataType = "java.lang.String", example = "test")
    private String projectName;


    @Data
    @Api(value = "DepDO", tags = "依赖DO")
    public static class DepDO {
        /**
         * 依赖任务的项目Id
         */
        @ApiModelProperty(name = "projectCode", value = "依赖任务的项目Id", dataType = "java.lang.Integer", example = "5")
        private int projectId;

        /**
         * 依赖任务的工作流Id
         */
        @ApiModelProperty(name = "processId", value = "依赖任务的工作流Id", dataType = "java.lang.Integer", example = "1")
        private Long processId;

        /**
         * 依赖任务的工作流名称
         */
        @ApiModelProperty(name = "processName", value = "依赖任务的工作流名称", dataType = "java.lang.String", example = "common")
        private String processName;

        /**
         * 依赖任务的Id
         */
        @ApiModelProperty(name = "taskId", value = "依赖任务的Id", dataType = "java.lang.String", example = "1")
        private String taskId;

        /**
         * 依赖任务的名称
         */
        @ApiModelProperty(name = "taskName", value = "依赖任务的名称", dataType = "java.lang.String", example = "test")
        private String taskName;

        /**
         * 任务全称
         */
        @ApiModelProperty(name = "fullTaskName", value = "依赖任务的全称", dataType = "java.lang.String", example = "测试工作流.测试任务")
        private String fullTaskName;

        /**
         * 表名
         */
        @ApiModelProperty(name = "tableName", value = "表名", dataType = "java.lang.String", example = "test_db.test_table")
        private String tableName;

        /**
         * 负责人姓名
         */
        @ApiModelProperty(name = "ownerName", value = "负责人姓名", dataType = "java.lang.String", example = "张三")
        private String ownerName;

        /**
         * 周期
         */
        @ApiModelProperty(name = "cycle", value = "依赖的任务周期", dataType = "java.lang.String", example = "每日")
        private String cycle;

        /**
         * dateValue
         */
        @ApiModelProperty(name = "dateValue", value = "依赖的任务周期值", dataType = "java.lang.String", example = "")
        private String dateValue;
    }
}
