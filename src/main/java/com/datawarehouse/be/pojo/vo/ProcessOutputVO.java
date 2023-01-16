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

import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ProcessOutputVO {

    private String outPutType;

    /**
     * 任务实例ID
     * */
    private int taskInstanceId;

    /**
     * 产出库名
     * */
    private String outputDatabaseName;

    /**
     * 产出表名
     * */
    private String outputTableName;

    /**
     * update_time
     * */
    private String endTime;

    /**
     * 执行结果
     * */
    private String status;

    public String getOutPutType() {
        return outPutType;
    }

    public void setOutPutType(String outPutType) {
        this.outPutType = outPutType;
    }

    public int getTaskInstanceId() {
        return taskInstanceId;
    }

    public void setTaskInstanceId(int taskInstanceId) {
        this.taskInstanceId = taskInstanceId;
    }

    public String getOutputDatabaseName() {
        return outputDatabaseName;
    }

    public void setOutputDatabaseName(String outputDatabaseName) {
        this.outputDatabaseName = outputDatabaseName;
    }

    public String getOutputTableName() {
        return outputTableName;
    }

    public void setOutputTableName(String outputTableName) {
        this.outputTableName = outputTableName;
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

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
