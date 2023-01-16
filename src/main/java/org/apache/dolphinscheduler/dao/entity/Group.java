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

import java.util.Date;

@TableName("bz_group")
public class Group {

    /**
     * id
     */
    @TableId(value="group_id", type=IdType.INPUT)
    private String groupId;

    private String groupName;
    private int projectId;
    private String initDatasourceIds;
    private String initUdfIds;
    private String initSourceIds;

    /**
     *  tenant id
     */
    private int tenantId;

    /**
     * create time
     */
    private Date createTime;

    /**
     * update time
     */
    private Date updateTime;


    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public String getInitDatasourceIds() {
        return initDatasourceIds;
    }

    public void setInitDatasourceIds(String initDatasourceIds) {
        this.initDatasourceIds = initDatasourceIds;
    }

    public String getInitUdfIds() {
        return initUdfIds;
    }

    public void setInitUdfIds(String initUdfIds) {
        this.initUdfIds = initUdfIds;
    }

    public String getInitSourceIds() {
        return initSourceIds;
    }

    public void setInitSourceIds(String initSourceIds) {
        this.initSourceIds = initSourceIds;
    }

    public int getTenantId() {
        return tenantId;
    }

    public void setTenantId(int tenantId) {
        this.tenantId = tenantId;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Group user = (Group) o;

        if (groupId != user.groupId) {
            return false;
        }
        return groupName.equals(user.groupName);

    }

    @Override
    public int hashCode() {
        String result = groupId;
        return result.hashCode() + groupName.hashCode();
    }

    @Override
    public String toString() {
        return "Group{" +
                "groupId=" + groupId +
                ", groupName='" + groupName + '\'' +
                ", projectId=" + projectId +
                ", initDatasourceIds='" + initDatasourceIds + '\'' +
                ", initUdfIds='" + initUdfIds + '\'' +
                ", initSourceIds='" + initSourceIds + '\'' +
                ", tenantId=" + tenantId +
                ", createTime=" + createTime +
                ", updateTime=" + updateTime +
                '}';
    }
}
