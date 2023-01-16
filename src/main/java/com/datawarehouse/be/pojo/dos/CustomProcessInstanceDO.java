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

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CustomProcessInstanceDO {

    private int processInstanceId;

    private String processName;

    private String processInstanceName;

    /**
     * project id
     */
    private Long projectId;

    /**
     * project id
     */
    private String projectName;

    /**
     * description
     */
    private String description;

    /**
     * sql
     * */
    private String sql;

    /**
     * create_time
     * */
    private String createTime;


    /**
     * update_time
     * */
    private String endTime;

    /**
     * process duration
     *
     * @return
     */
    private String duration;


    private String scheduleTime;

    private String executorName;

    public int getProcessInstanceId() {
        return processInstanceId;
    }

    public void setProcessInstanceId(int processInstanceId) {
        this.processInstanceId = processInstanceId;
    }

    public String getProcessName() {
        return processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    public String getProcessInstanceName() {
        return processInstanceName;
    }

    public void setProcessInstanceName(String processInstanceName) {
        this.processInstanceName = processInstanceName;
    }

    public Long getProjectId() {
        return projectId;
    }

    public void setProjectId(Long projectId) {
        this.projectId = projectId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        if(createTime == null){
            this.createTime = "-";
        }else{
            Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.createTime = format.format(createTime);
        }

    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        if(endTime == null){
            this.endTime = "-";
        }else{
            Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.endTime = format.format(endTime);
        }

    }

    public String getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(Date scheduleTime) {
        if(scheduleTime == null){
            this.scheduleTime = "-";
        }else{
            Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.scheduleTime = format.format(scheduleTime);
        }
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getExecutorName() {
        return executorName;
    }

    public void setExecutorName(String executorName) {
        this.executorName = executorName;
    }
}
