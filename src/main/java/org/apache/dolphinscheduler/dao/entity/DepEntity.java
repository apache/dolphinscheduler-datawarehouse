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

package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

@TableName("bz_zr_process_dep")
public class DepEntity {
    /**
     * id
     */
    @TableId(value="id", type= IdType.AUTO)
    private int id;

    /**
     * 依赖任务的项目Id
     */
    private int projectId;
    /**
     * 依赖任务的工作流Id
     */
    private Long processId;
    /**
     * 依赖任务的工作流名称
     */
    private String processName;
    /**
     * 任务名称
     */
    private String taskName;
    /**
     * 任务全称
     */
    private String fullTaskName;
    /**
     * 表名
     */
    private String tableName;
    /**
     * 负责人姓名
     */
    private String ownerName;
    /**
     * 周期
     */
    private String cycle;
    /**
     * 周期值
     */
    private String dateValue;

    /**
     * 所属的工作流Id
     */
    private Long owningProcessId;

    public DepEntity(int id, int projectId, Long processId, String processName, String taskName, String fullTaskName, String tableName, String ownerName, String cycle, String dateValue, Long owningProcessId) {
        this.id = id;
        this.projectId = projectId;
        this.processId = processId;
        this.processName = processName;
        this.taskName = taskName;
        this.fullTaskName = fullTaskName;
        this.tableName = tableName;
        this.ownerName = ownerName;
        this.cycle = cycle;
        this.dateValue = dateValue;
        this.owningProcessId = owningProcessId;
    }

    public void wrap(Long owningProcessId){
        this.owningProcessId = owningProcessId;
    }

    public DepEntity() {
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public Long getProcessId() {
        return processId;
    }

    public void setProcessId(Long processId) {
        this.processId = processId;
    }

    public String getProcessName() {
        return processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getFullTaskName() {
        return fullTaskName;
    }

    public void setFullTaskName(String fullTaskName) {
        this.fullTaskName = fullTaskName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getOwnerName() {
        return ownerName;
    }

    public void setOwnerName(String ownerName) {
        this.ownerName = ownerName;
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

    public Long getOwningProcessId() {
        return owningProcessId;
    }

    public void setOwningProcessId(Long owningProcessId) {
        this.owningProcessId = owningProcessId;
    }
}
