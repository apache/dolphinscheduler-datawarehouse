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
import com.google.common.collect.Lists;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;

import java.util.List;
import java.util.Random;

@Api(value = "ProcessJsonDO", tags = "DS中Process对象")
public class ProcessJsonDevBranchDO {

    /**
     * tasks
     */
    @ApiModelProperty(name = "tasks", value = "任务集合", dataType = "java.lang.List<TasksDTO>", example = "", required = true)
    @JsonProperty("tasks")
    private List<TasksDTO> tasks;
    /**
     * globalParams
     */
    @ApiModelProperty(name = "globalParams", value = "工作流全局参数，默认为空对象", dataType = "java.lang.List", example = "", hidden = true)
    @JsonProperty("globalParams")
    private List<?> globalParams;
    /**
     * timeout
     */
    @ApiModelProperty(name = "timeout", value = "工作流超时时间", dataType = "java.lang.Integer", example = "0")
    @JsonProperty("timeout")
    private Integer timeout;
    /**
     * tenantId
     */
    @ApiModelProperty(name = "tenantId", value = "租户Id(默认为1，数据平台)", dataType = "java.lang.Integer", hidden = true)
    @JsonProperty("tenantId")
    private Integer tenantId;

    public void wrap(Integer tenantId) {
        this.setTenantId(tenantId);
    }

    public void init() {
        this.setGlobalParams(Lists.newArrayList());
        this.setTimeout(0);
        this.setTenantId(1);
    }

    public List<TasksDTO> getTasks() {
        return tasks;
    }

    public void setTasks(List<TasksDTO> tasks) {
        this.tasks = tasks;
    }

    public List<?> getGlobalParams() {
        return globalParams;
    }

    public void setGlobalParams(List<?> globalParams) {
        this.globalParams = globalParams;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Integer getTenantId() {
        return tenantId;
    }

    public void setTenantId(Integer tenantId) {
        this.tenantId = tenantId;
    }

    @Api(value = "TasksDTO", tags = "任务对象")
    public static class TasksDTO<T> {
        /**
         * id
         */
        @ApiModelProperty(name = "id", value = "任务Id", dataType = "java.lang.Integer", hidden = true)
        @JsonProperty("id")
        private String id;
        /**
         * name
         */
        @ApiModelProperty(name = "name", value = "任务名称", dataType = "java.lang.String", required = true)
        @JsonProperty("name")
        private String name;
        /**
         * desc
         */
        @ApiModelProperty(name = "desc", value = "任务描述", dataType = "java.lang.String")
        @JsonProperty("desc")
        private String desc;
        /**
         * type
         */
        @ApiModelProperty(name = "type", value = "任务类型（DEPENDENT、SQL）", dataType = "java.lang.String", required = true)
        @JsonProperty("type")
        private String type;
        /**
         * runFlag
         */
        @ApiModelProperty(name = "runFlag", value = "运行标识", dataType = "java.lang.String")
        @JsonProperty("runFlag")
        private String runFlag;
        /**
         * loc
         */
        @ApiModelProperty(name = "loc", value = "任务的坐标", dataType = "java.lang.Object", hidden = true)
        @JsonProperty("loc")
        private Object loc;
        /**
         * maxRetryTimes
         */
        @ApiModelProperty(name = "maxRetryTimes", value = "任务最大重试次数", dataType = "java.lang.Integer", required = true)
        @JsonProperty("maxRetryTimes")
        private Integer maxRetryTimes;
        /**
         * retryInterval
         */
        @ApiModelProperty(name = "retryInterval", value = "任务重试间隔时间（分钟）", dataType = "java.lang.Integer", required = true)
        @JsonProperty("retryInterval")
        private Integer retryInterval;
        /**
         * params
         */
        @ApiModelProperty(name = "params", value = "动作对象", dataType = "ParamsDTO", required = true)
        @JsonProperty("params")
        private T params;
        /**
         * preTasks
         */
        @ApiModelProperty(name = "loc", value = "前置任务", dataType = "java.util.List", hidden = true)
        @JsonProperty("preTasks")
        private List<String> preTasks;
        /**
         * extras
         */
        @ApiModelProperty(name = "extras", value = "额外", dataType = "java.util.Object", hidden = true)
        @JsonProperty("extras")
        private Object extras;
        /**
         * depList
         */
        @ApiModelProperty(name = "depList", value = "任务依赖集合", dataType = "java.util.List")
        @JsonProperty("depList")
        private List<String> depList;
        /**
         * dependence
         */
        @ApiModelProperty(name = "dependence", value = "条件对象", dataType = "DependenceDTO", required = true)
        @JsonProperty("dependence")
        private DependenceDTO dependence;
        /**
         * conditionResult
         */
        @ApiModelProperty(name = "conditionResult", value = "任务结果处理对象", dataType = "ConditionResultDTO", hidden = true)
        @JsonProperty("conditionResult")
        private ConditionResultDTO conditionResult = new ConditionResultDTO();
        /**
         * taskInstancePriority
         */
        @ApiModelProperty(name = "taskInstancePriority", value = "任务优先级，默认为MEDIUM", dataType = "java.lang.String", example = "MEDIUM")
        @JsonProperty("taskInstancePriority")
        private String taskInstancePriority;
        /**
         * workerGroup
         */
        @ApiModelProperty(name = "workerGroup", value = "队列名称，默认值default，业务线DAG迁移前约定好值", dataType = "java.lang.String", required = true)
        @JsonProperty("workerGroup")
        private String workerGroup;
        /**
         * workerGroupId
         */
        @ApiModelProperty(name = "workerGroupId", value = "队列Id，由DS生成", dataType = "java.lang.String", hidden = true)
        @JsonProperty("workerGroupId")
        private Object workerGroupId;
        /**
         * timeout
         */
        @ApiModelProperty(name = "timeout", value = "任务超时处理", dataType = "java.lang.TimeoutDTO", hidden = true)
        @JsonProperty("timeout")
        private TimeoutDTO timeout = new TimeoutDTO();
        /**
         * delayTime
         */
        @ApiModelProperty(name = "delayTime", value = "任务默认延时时间，默认0", dataType = "java.lang.Integer")
        @JsonProperty("delayTime")
        private Integer delayTime;

        public void wrap(String name){
            this.name = name;
        }

        public void init(){
            this.setId("Tasks-" + new Random().nextInt(99999 - 0 + 1) + 0);
            this.setRunFlag("NORMAL");
            this.setParams((T) new Object());
            this.setPreTasks(Lists.newArrayList());
            this.setDepList(Lists.newArrayList());
            this.setTaskInstancePriority("MEDIUM");
            this.setWorkerGroup("default");
            this.setDelayTime(0);
        }

        public void wrap(String name, String type, Integer maxRetryTimes, Integer retryInterval, String workerGroup) {
            this.name = name;
            this.type = type;
            this.maxRetryTimes = maxRetryTimes;
            this.retryInterval = retryInterval;
            this.workerGroup = workerGroup;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getDesc() {
            return desc;
        }

        public void setDesc(String desc) {
            this.desc = desc;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getRunFlag() {
            return runFlag;
        }

        public void setRunFlag(String runFlag) {
            this.runFlag = runFlag;
        }

        public Object getLoc() {
            return loc;
        }

        public void setLoc(Object loc) {
            this.loc = loc;
        }

        public Integer getMaxRetryTimes() {
            return maxRetryTimes;
        }

        public void setMaxRetryTimes(Integer maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
        }

        public Integer getRetryInterval() {
            return retryInterval;
        }

        public void setRetryInterval(Integer retryInterval) {
            this.retryInterval = retryInterval;
        }

        public T getParams() {
            return params;
        }

        public void setParams(T params) {
            this.params = params;
        }

        public List<String> getPreTasks() {
            return preTasks;
        }

        public void setPreTasks(List<String> preTasks) {
            this.preTasks = preTasks;
        }

        public Object getExtras() {
            return extras;
        }

        public void setExtras(Object extras) {
            this.extras = extras;
        }

        public List<String> getDepList() {
            return depList;
        }

        public void setDepList(List<String> depList) {
            this.depList = depList;
        }

        public DependenceDTO getDependence() {
            return dependence;
        }

        public void setDependence(DependenceDTO dependence) {
            this.dependence = dependence;
        }

        public ConditionResultDTO getConditionResult() {
            return conditionResult;
        }

        public void setConditionResult(ConditionResultDTO conditionResult) {
            this.conditionResult = conditionResult;
        }

        public String getTaskInstancePriority() {
            return taskInstancePriority;
        }

        public void setTaskInstancePriority(String taskInstancePriority) {
            this.taskInstancePriority = taskInstancePriority;
        }

        public String getWorkerGroup() {
            return workerGroup;
        }

        public void setWorkerGroup(String workerGroup) {
            this.workerGroup = workerGroup;
        }

        public Object getWorkerGroupId() {
            return workerGroupId;
        }

        public void setWorkerGroupId(Object workerGroupId) {
            this.workerGroupId = workerGroupId;
        }

        public TimeoutDTO getTimeout() {
            return timeout;
        }

        public void setTimeout(TimeoutDTO timeout) {
            this.timeout = timeout;
        }

        public Integer getDelayTime() {
            return delayTime;
        }

        public void setDelayTime(Integer delayTime) {
            this.delayTime = delayTime;
        }


        @Api(value = "PythonParamsDTO", tags = "Python动作对象")
        public static class PythonParamsDTO {

            /**
             * resourceList
             */
            @JsonProperty("resourceList")
            private List<ResourceListDTO> resourceList;
            /**
             * localParams
             */
            @JsonProperty("localParams")
            private List<?> localParams;
            /**
             * rawScript
             */
            @JsonProperty("rawScript")
            private String rawScript;

            public List<ResourceListDTO> getResourceList() {
                return resourceList;
            }

            public void setResourceList(List<ResourceListDTO> resourceList) {
                this.resourceList = resourceList;
            }

            public List<?> getLocalParams() {
                return localParams;
            }

            public void setLocalParams(List<?> localParams) {
                this.localParams = localParams;
            }

            public String getRawScript() {
                return rawScript;
            }

            public void setRawScript(String rawScript) {
                this.rawScript = rawScript;
            }

            public static class ResourceListDTO {
                /**
                 * id
                 */
                @JsonProperty("id")
                private Integer id;

                public Integer getId() {
                    return id;
                }

                public void setId(Integer id) {
                    this.id = id;
                }
            }
        }

        @Api(value = "SQLParamsDTO", tags = "SQL动作对象")
        public static class SQLParamsDTO {
            /**
             * type
             */
            @ApiModelProperty(name = "type", value = "动作类型，默认HIVE", dataType = "java.lang.String", example = "SQL")
            @JsonProperty("type")
            private String type = "HIVE";
            /**
             * datasource
             */
            @ApiModelProperty(name = "datasource", value = "数据源，默认值为1（Hive数据源），迁移业务线之前约定好值", dataType = "java.lang.Integer", required = true)
            @JsonProperty("datasource")
            private Integer datasource = 1;
            /**
             * sql
             */
            @ApiModelProperty(name = "sql", value = "查询语句", dataType = "java.lang.String", required = true)
            @JsonProperty("sql")
            private String sql;
            /**
             * udfs
             */
            @ApiModelProperty(name = "udfs", value = "自定义函数,默认为空字符串", dataType = "java.lang.String")
            @JsonProperty("udfs")
            private String udfs = "";
            /**
             * sqlType
             */
            @ApiModelProperty(name = "sqlType", value = "SQL类型，默认为1，非查询，0为查询", dataType = "java.lang.String", example = "1")
            @JsonProperty("sqlType")
            private String sqlType = "1";
            /**
             * sendEmail
             */
            @ApiModelProperty(name = "sendEmail", value = "发送邮件，默认为false", dataType = "java.lang.Boolean")
            @JsonProperty("sendEmail")
            private Boolean sendEmail = false;
            /**
             * displayRows
             */
            @ApiModelProperty(name = "displayRows", value = "显示行数，默认10行", dataType = "java.lang.Integer")
            @JsonProperty("displayRows")
            private Integer displayRows = 10;
            /**
             * title
             */
            @ApiModelProperty(name = "title", value = "标题，默认为空字符串", dataType = "java.lang.String")
            @JsonProperty("title")
            private String title = "";
            /**
             * groupId
             */
            @ApiModelProperty(name = "groupId", value = "分组Id", dataType = "java.lang.Object", hidden = true)
            @JsonProperty("groupId")
            private Object groupId;
            /**
             * localParams
             */
            @ApiModelProperty(name = "localParams", value = "任务参数", dataType = "java.util.List")
            @JsonProperty("localParams")
            private List<String> localParams = Lists.newArrayList();
            /**
             * connParams
             */
            @ApiModelProperty(name = "connParams", value = "连接参数", dataType = "java.lang.String")
            @JsonProperty("connParams")
            private String connParams = "";
            /**
             * preStatements
             */
            @ApiModelProperty(name = "preStatements", value = "前置SQL，由DS处理，解决多条SQL的问题", dataType = "java.util.List")
            @JsonProperty("preStatements")
            private List<String> preStatements = Lists.newArrayList();
            /**
             * postStatements
             */
            @ApiModelProperty(name = "postStatements", value = "后置SQL，由DS处理，解决多条SQL的问题", dataType = "java.util.List")
            @JsonProperty("postStatements")
            private List<String> postStatements = Lists.newArrayList();

            public void wrap(String sql, Integer datasource, String type) {
                this.setSql(sql);
                this.setDatasource(datasource);
                this.setType(type);
            }

            public void wrap(Integer datasource, String type) {
                this.setDatasource(datasource);
                this.setType(type);
            }

            public String getType() {
                return type;
            }

            public void setType(String type) {
                this.type = type;
            }

            public Integer getDatasource() {
                return datasource;
            }

            public void setDatasource(Integer datasource) {
                this.datasource = datasource;
            }

            public String getSql() {
                return sql;
            }

            public void setSql(String sql) {
                this.sql = sql;
            }

            public String getUdfs() {
                return udfs;
            }

            public void setUdfs(String udfs) {
                this.udfs = udfs;
            }

            public String getSqlType() {
                return sqlType;
            }

            public void setSqlType(String sqlType) {
                this.sqlType = sqlType;
            }

            public Boolean getSendEmail() {
                return sendEmail;
            }

            public void setSendEmail(Boolean sendEmail) {
                this.sendEmail = sendEmail;
            }

            public Integer getDisplayRows() {
                return displayRows;
            }

            public void setDisplayRows(Integer displayRows) {
                this.displayRows = displayRows;
            }

            public String getTitle() {
                return title;
            }

            public void setTitle(String title) {
                this.title = title;
            }

            public Object getGroupId() {
                return groupId;
            }

            public void setGroupId(Object groupId) {
                this.groupId = groupId;
            }

            public List<?> getLocalParams() {
                return localParams;
            }

            public void setLocalParams(List<String> localParams) {
                this.localParams = localParams;
            }

            public String getConnParams() {
                return connParams;
            }

            public void setConnParams(String connParams) {
                this.connParams = connParams;
            }

            public List<?> getPreStatements() {
                return preStatements;
            }

            public void setPreStatements(List<String> preStatements) {
                this.preStatements = preStatements;
            }

            public List<?> getPostStatements() {
                return postStatements;
            }

            public void setPostStatements(List<String> postStatements) {
                this.postStatements = postStatements;
            }
        }

        @Api(value = "DependenceDTO", tags = "依赖对象")
        public static class DependenceDTO {
            /**
             * relation
             */
            @ApiModelProperty(name = "relation", value = "连接条件，默认AND", dataType = "java.lang.String")
            @JsonProperty("relation")
            private String relation;
            /**
             * dependTaskList
             */
            @ApiModelProperty(name = "dependTaskList", value = "条件任务集合", dataType = "java.util.List", required = true)
            @JsonProperty("dependTaskList")
            private List<DependTaskListDTO> dependTaskList ;

            public void init(){
                this.setRelation("AND");
                this.setDependTaskList(Lists.newArrayList());
            }

            public String getRelation() {
                return relation;
            }

            public void setRelation(String relation) {
                this.relation = relation;
            }

            public List<DependTaskListDTO> getDependTaskList() {
                return dependTaskList;
            }

            public void setDependTaskList(List<DependTaskListDTO> dependTaskList) {
                this.dependTaskList = dependTaskList;
            }

            @Api(value = "DependTaskListDTO", tags = "依赖任务集合")
            public static class DependTaskListDTO {
                /**
                 * relation
                 */
                @ApiModelProperty(name = "relation", value = "连接条件，默认AND", dataType = "java.lang.String")
                @JsonProperty("relation")
                private String relation;
                /**
                 * dependItemList
                 */
                @ApiModelProperty(name = "dependItemList", value = "条件项集合", dataType = "java.util.List", required = true)
                @JsonProperty("dependItemList")
                private List<DependItemListDTO> dependItemList;

                public void init(){
                    this.setRelation("AND");
                    this.setDependItemList(Lists.newArrayList());
                }

                public String getRelation() {
                    return relation;
                }

                public void setRelation(String relation) {
                    this.relation = relation;
                }

                public List<DependItemListDTO> getDependItemList() {
                    return dependItemList;
                }

                public void setDependItemList(List<DependItemListDTO> dependItemList) {
                    this.dependItemList = dependItemList;
                }

                @Api(value = "DependItemListDTO", tags = "依赖项集合")
                public static class DependItemListDTO {
                    /**
                     * projectCode
                     */
                    @ApiModelProperty(name = "projectCode", value = "项目Id,业务线DAG迁移前约定好值", dataType = "java.lang.Integer")
                    @JsonProperty("projectCode")
                    private Integer projectId;
                    /**
                     * definitionCode
                     */
                    @ApiModelProperty(name = "definitionCode", value = "工作流Id,业务线DAG迁移前约定好值", dataType = "java.lang.Integer")
                    @JsonProperty("definitionCode")
                    private Integer definitionId;
                    /**
                     * depTasks
                     */
                    @ApiModelProperty(name = "depTasks", value = "依赖的任务名称,DS后台来设置", dataType = "java.lang.String", required = true)
                    @JsonProperty("depTasks")
                    private String depTasks;
                    /**
                     * cycle
                     */
                    @ApiModelProperty(name = "cycle", value = "依赖的任务周期,DS后台来设置", dataType = "java.lang.String", hidden = true)
                    @JsonProperty("cycle")
                    private String cycle;
                    /**
                     * dateValue
                     */
                    @ApiModelProperty(name = "dateValue", value = "依赖的任务周期值,DS后台来设置", dataType = "java.lang.String", hidden = true)
                    @JsonProperty("dateValue")
                    private String dateValue;

                    public DependItemListDTO(Integer projectId, Integer definitionId, String depTasks, String cycle, String dateValue) {
                        this.projectId = projectId;
                        this.definitionId = definitionId;
                        this.depTasks = depTasks;
                        this.cycle = cycle;
                        this.dateValue = dateValue;
                    }

                    public DependItemListDTO() {
                    }

                    public void wrap(String depName) {
                        this.setDepTasks(depName);
                    }

                    public Integer getProjectId() {
                        return projectId;
                    }

                    public void setProjectId(Integer projectId) {
                        this.projectId = projectId;
                    }

                    public Integer getDefinitionId() {
                        return definitionId;
                    }

                    public void setDefinitionId(Integer definitionId) {
                        this.definitionId = definitionId;
                    }

                    public String getDepTasks() {
                        return depTasks;
                    }

                    public void setDepTasks(String depTasks) {
                        this.depTasks = depTasks;
                    }

                    public String getCycle() {
                        return cycle;
                    }

                    public void setCycle(String cycle) {
                        this.cycle = cycle;
                    }

                    public String getDateValue() {
                        return dateValue;
                    }

                    public void setDateValue(String dateValue) {
                        this.dateValue = dateValue;
                    }
                }
            }
        }

        public static class ConditionResultDTO {
            /**
             * successNode
             */
            @JsonProperty("successNode")
            private List<String> successNode = Lists.newArrayList("");
            /**
             * failedNode
             */
            @JsonProperty("failedNode")
            private List<String> failedNode = Lists.newArrayList("");

            public List<String> getSuccessNode() {
                return successNode;
            }

            public void setSuccessNode(List<String> successNode) {
                this.successNode = successNode;
            }

            public List<String> getFailedNode() {
                return failedNode;
            }

            public void setFailedNode(List<String> failedNode) {
                this.failedNode = failedNode;
            }
        }

        public static class TimeoutDTO {
            /**
             * strategy
             */
            @JsonProperty("strategy")
            private String strategy = "";
            /**
             * interval
             */
            @JsonProperty("interval")
            private Object interval;
            /**
             * enable
             */
            @JsonProperty("enable")
            private Boolean enable = false;

            public String getStrategy() {
                return strategy;
            }

            public void setStrategy(String strategy) {
                this.strategy = strategy;
            }

            public Object getInterval() {
                return interval;
            }

            public void setInterval(Object interval) {
                this.interval = interval;
            }

            public Boolean getEnable() {
                return enable;
            }

            public void setEnable(Boolean enable) {
                this.enable = enable;
            }
        }
    }
}
