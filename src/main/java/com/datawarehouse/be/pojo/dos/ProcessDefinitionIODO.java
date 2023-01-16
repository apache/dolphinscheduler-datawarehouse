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

public class ProcessDefinitionIODO {
    /**
     * 工作流id
     */
    private Long processDefinitionId;

    /**
     * 工作流名称
     */
    private String processDefinitionName;

    /**
     * 任务id
     */
    private String taskId;

    /**
     * 任务名称
     */
    private String taskName;

    /**
     * 输入/输出名称
     */
    private String ioName;

    public ProcessDefinitionIODO(Long processDefinitionId, String processDefinitionName, String taskId, String taskName, String ioName) {
        this.processDefinitionId = processDefinitionId;
        this.processDefinitionName = processDefinitionName;
        this.taskId = taskId;
        this.taskName = taskName;
        this.ioName = ioName;
    }

    public ProcessDefinitionIODO(Long processDefinitionId, String processDefinitionName, String taskId, String taskName) {
        this.processDefinitionId = processDefinitionId;
        this.processDefinitionName = processDefinitionName;
        this.taskId = taskId;
        this.taskName = taskName;
    }

    public ProcessDefinitionIODO() {
    }

    public String getIoName() {
        return ioName;
    }

    public void setIoName(String ioName) {
        this.ioName = ioName;
    }

    public Long getProcessDefinitionId() {
        return processDefinitionId;
    }

    public void setProcessDefinitionId(Long processDefinitionId) {
        this.processDefinitionId = processDefinitionId;
    }

    public String getProcessDefinitionName() {
        return processDefinitionName;
    }

    public void setProcessDefinitionName(String processDefinitionName) {
        this.processDefinitionName = processDefinitionName;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }
}
