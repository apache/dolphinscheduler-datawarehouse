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

import com.datawarehouse.be.exceptions.ProcessParamsException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.StringUtils;

import java.util.List;

@NoArgsConstructor
@Data
@Api(value = "SqlParsingVO", tags = "SQL解析VO")
public class SqlParsingVO {

    @ApiModelProperty(name = "depDOList", value = "依赖集合", dataType = "java.lang.List<DepDO>", example = "")
    private List<DepDO> depDOList;

    @ApiModelProperty(name = "outPutDOList", value = "导出DO", dataType = "java.lang.List<ExportDO>", example = "")
    private List<OutPutDO> outPutDOList;

    @ApiModelProperty(name = "warningMsg", value = "警告信息", dataType = "java.lang.String", example = "")
    private String warningMsg;

    public SqlParsingVO(List<DepDO> depDOList, List<OutPutDO> outPutDOList, String warningMsg) {
        this.depDOList = depDOList;
        this.outPutDOList = outPutDOList;
        this.warningMsg = warningMsg;
    }

    @Data
    @Api(value = "ExportDO", tags = "导出DO")
    public static class ExportDO<T> {
        /**
         * 数据源名称
         */
        @ApiModelProperty(name = "datasourceName", value = "数据源名称", dataType = "java.lang.String", example = "hive-pro")
        private String datasourceName;

        /**
         * 数据源Json对象
         */
        @ApiModelProperty(name = "datasourceJson", value = "数据源Json对象", dataType = "java.lang.String", example = "\"{\"retentionDays\":1}\"")
        private String datasourceJson;

        /**
         * 数据源对象
         */
        @ApiModelProperty(name = "datasource", value = "数据源对象", dataType = "java.lang.Object", hidden = true)
        private T datasource;

        public void wrap(String datasourceName, String datasourceJson){
            this.datasourceName = datasourceName;
            this.datasourceJson = datasourceJson;
        }


        @Data
        @Api(value = "ClickhouseDO", tags = "Clickhouse数据源")
        public static class ClickhouseDO {
            /**
             * 保留天数
             */
            @ApiModelProperty(name = "RetentionDays", value = "保留天数", dataType = "java.lang.Integer", example = "1")
            private Integer retentionDays;
        }
    }

    @Data
    @Api(value = "DepDO", tags = "依赖DO")
    public static class DepDO {

        /**
         * 依赖任务的项目Id
         */
        @ApiModelProperty(name = "projectCode", value = "依赖任务的项目Id", dataType = "java.lang.Integer", example = "5")
        private long projectId;
        /**
         * 依赖任务的工作流Id
         */
        @ApiModelProperty(name = "processId", value = "依赖任务的工作流Id", dataType = "java.lang.Integer", example = "71")
        private long processId;
        /**
         * 依赖任务的工作流名称
         */
        @ApiModelProperty(name = "processName", value = "依赖任务的工作流名称", dataType = "java.lang.String", example = "common")
        private String processName;
        /**
         * 任务id
         */
        @ApiModelProperty(name = "taskId", value = "依赖任务的Id", dataType = "java.lang.String", example = "1")
        private String taskId;
        /**
         * 任务名称
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
         * 周期是否为每天
         */
        @ApiModelProperty(name = "isDayOfCycle", value = "周期是否为每天，0=否，1=是", dataType = "java.lang.Integer", example = "0")
        private int isDayOfCycle;


        public DepDO(long projectId, long processId, String processName, String taskId, String taskName, String tableName, String ownerName, int isDayOfCycle) {
            this.projectId = projectId;
            this.processId = processId;
            this.processName = processName;
            this.taskId = taskId;
            this.taskName = taskName;
            this.fullTaskName = new StringBuffer(StringUtils.isNotBlank(processName) ? processName : "无").append(".").append(StringUtils.isNotBlank(taskName) ? taskName.replace(".$[yyyyMMdd-1].sensor", "") : "无").toString();
            this.tableName = tableName;
            this.ownerName = ownerName;
            this.isDayOfCycle = isDayOfCycle;
        }

        public DepDO(long processId, String processName, String taskId, String taskName, String fullTaskName, String tableName, String ownerName) {
            this.processId = processId;
            this.processName = processName;
            this.taskId = taskId;
            this.taskName = taskName;
            this.fullTaskName = fullTaskName;
            this.tableName = tableName;
            this.ownerName = ownerName;
        }

        public DepDO() {
        }
    }

    @Data
    @Api(value = "OutPutDO", tags = "输出DO")
    public static class OutPutDO {
        /**
         * 库名
         */
        @ApiModelProperty(name = "dbName", value = "数据库名称", dataType = "java.lang.String", example = "test")
        private String dbName;
        /**
         * 表名
         */
        @ApiModelProperty(name = "tableName", value = "表名", dataType = "java.lang.String", example = "opentsdb")
        private String tableName;
        /**
         * 表类型
         */
        @ApiModelProperty(name = "tableType", value = "表类型", dataType = "java.lang.String", example = "分区表")
        private String tableType;
        /**
         * 数据时效（天数）
         */
        @ApiModelProperty(name = "validDays", value = "数据时效（天数）", dataType = "java.lang.Integer", example = "3", hidden = true)
        private Integer validDays;

        public OutPutDO(String dbName, String tableName, String tableType) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.tableType = tableType;
        }

        public OutPutDO() {
        }

        public void verify(){
            if (StringUtils.isBlank(this.dbName)) {
                throw new ProcessParamsException(String.format("输出中数据库名[dbName]不能为空，请确认~", this.dbName));
            }
            if (StringUtils.isBlank(this.tableName)) {
                throw new ProcessParamsException(String.format("输出中表名[tableName]不能为空，请确认~", this.tableName));
            }
        }
    }
}
