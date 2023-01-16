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

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

@TableName("bz_zr_process_enwechat")
public class ProcessEnwechat {

    @TableId("id")
    private int id;

    /**
     * 工作流ID
     */
    private Long processId;

    /**
     * 企业微信账户
     */
    private String enterpriseWechatAccount;


    public ProcessEnwechat() {
    }

    public ProcessEnwechat(int id, Long processId, String enterpriseWechatAccount) {
        this.id = id;
        this.processId = processId;
        this.enterpriseWechatAccount = enterpriseWechatAccount;
    }

    public ProcessEnwechat(Long processId, String enterpriseWechatAccount) {
        this.processId = processId;
        this.enterpriseWechatAccount = enterpriseWechatAccount;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Long getProcessId() {
        return processId;
    }

    public void setProcessId(Long processId) {
        this.processId = processId;
    }

    public String getEnterpriseWechatAccount() {
        return enterpriseWechatAccount;
    }

    public void setEnterpriseWechatAccount(String enterpriseWechatAccount) {
        this.enterpriseWechatAccount = enterpriseWechatAccount;
    }
}
