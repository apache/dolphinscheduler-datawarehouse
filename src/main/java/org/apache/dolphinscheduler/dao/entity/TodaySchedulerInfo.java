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

import java.util.Objects;

/**
 * @Author LiangYansheng
 * @Date 2021/9/8
 */
public class TodaySchedulerInfo {

    /** 今日调度任务数 **/
    private int todayScheduleNum;
    /** 今日启动任务数 **/
    private int exeNum;
    /** 执行失败任务数 **/
    private int failureNum;

    public int getTodayScheduleNum() {
        return todayScheduleNum;
    }

    public void setTodayScheduleNum(int todayScheduleNum) {
        this.todayScheduleNum = todayScheduleNum;
    }

    public int getExeNum() {
        return exeNum;
    }

    public void setExeNum(int exeNum) {
        this.exeNum = exeNum;
    }

    public int getFailureNum() {
        return failureNum;
    }

    public void setFailureNum(int failureNum) {
        this.failureNum = failureNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TodaySchedulerInfo that = (TodaySchedulerInfo) o;
        return todayScheduleNum == that.todayScheduleNum && exeNum == that.exeNum && failureNum == that.failureNum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(todayScheduleNum, exeNum, failureNum);
    }

    @Override
    public String toString() {
        return "TodaySchedulerInfo{" +
                "todayScheduleNum=" + todayScheduleNum +
                ", exeNum=" + exeNum +
                ", failureNum=" + failureNum +
                '}';
    }
}
