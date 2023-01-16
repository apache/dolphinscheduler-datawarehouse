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

package com.datawarehouse.be.enums;

import com.baomidou.mybatisplus.annotation.EnumValue;
import org.apache.dolphinscheduler.common.enums.ExecutionStatus;

/**
 * running status for workflow and task nodes
 *
 */
public enum ExecutionStatusEnums {
    ZR_RUNNING(1, "执行中"),
    ZR_PAUSE(2, "暂停"),
    ZR_STOP(4, "停止"),
    ZR_FAILURE(6, "失败"),
    ZR_SUCCESS(7, "已完成"),
    ZR_UNKNOWN(0, "未知");

    ExecutionStatusEnums(int code, String descp){
        this.code = code;
        this.descp = descp;
    }

    @EnumValue
    private final int code;
    private final String descp;

    public int getCode() {
        return code;
    }

    public String getDescp() {
        return descp;
    }

    /**
     * 根据ds任务状态，获取自如任务状态
     * */
    public static ExecutionStatusEnums getZiroomTaskStatus(ExecutionStatus executionStatus){
        switch (executionStatus) {

            //0,1,8,10,11 对应执行中
            case SUBMITTED_SUCCESS:
            case RUNNING_EXECUTION:
            case NEED_FAULT_TOLERANCE:
            case WAITING_THREAD:
            case WAITING_DEPEND:
                return ZR_RUNNING;

            //2,3 对应暂停
            case READY_PAUSE:
            case PAUSE:
                return ZR_PAUSE;

            //4,5 对应停止
            case READY_STOP:
            case STOP:
                return ZR_STOP;

            case FAILURE:
                return ZR_FAILURE;

            case SUCCESS:
                return ZR_SUCCESS;

            //其他所有对应未知
            default :
                return ZR_UNKNOWN;
        }

    }

}
