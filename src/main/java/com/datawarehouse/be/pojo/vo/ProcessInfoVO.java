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

import org.apache.dolphinscheduler.common.enums.ReleaseState;

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ProcessInfoVO {

    /**
     * id
     */
    private int id;

    /**
     * name
     */
    private String name;

    /**
     * version
     */
    private long version;

    /**
     * release state : online/offline
     */
    private ReleaseState releaseState;

    /**
     * project id
     */
    private int projectId;

    /**
     * description
     */
    private String description;

    /**
     * sql
     * */
    private String sql;

    /**
     * corn
     * */
    private String cron;

    /**
     * toStream，产出信息
     * */
    private String toStream;

    /**
     * create_time
     * */
    private String createTime;


    /**
     * update_time
     * */
    private String updateTime;

    /**
     * 修改人
     * */
    private String modifyBy;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public ReleaseState getReleaseState() {
        return releaseState;
    }

    public void setReleaseState(ReleaseState releaseState) {
        this.releaseState = releaseState;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
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

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public String getToStream() {
        return toStream;
    }

    public void setToStream(String toStream) {
        this.toStream = toStream;
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

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        if(updateTime == null){
            this.updateTime = "-";
        }else{
            Format format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            this.updateTime = format.format(updateTime);
        }
    }

    public String getModifyBy() {
        return modifyBy;
    }

    public void setModifyBy(String modifyBy) {
        this.modifyBy = modifyBy;
    }
}










