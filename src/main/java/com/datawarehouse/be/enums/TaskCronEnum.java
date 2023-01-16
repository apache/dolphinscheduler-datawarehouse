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

import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.function.Function;

@Getter
public enum TaskCronEnum {
    HOUR("时", "hour", (dateValue) -> {
        String result = null;
        switch (dateValue) {
            case "当前小时":
                result = "currentHour";
                break;
            case "前1小时":
                result = "last1Hour";
                break;
            case "前2小时":
                result= "last2Hours";
                break;
            case "前3小时":
                result = "last3Hours";
                break;
            case "前24小时":
                result = "last24Hours";
                break;
            case "currentHour":
                result = "当前小时";
                break;
            case "last1Hour":
                result = "前1小时";
                break;
            case "last2Hours":
                result= "前2小时";
                break;
            case "last3Hours":
                result = "前3小时";
                break;
            case "last24Hours":
                result = "前24小时";
                break;
            default:
                break;
        }
        return result;
    }),
    DAY("日", "day", (dateValue) -> {
        String result = null;
        switch (dateValue){
            case "今天":
                result = "today";
                break;
            case "昨天":
                result = "last1Days";
                break;
            case "前2天":
                result = "last2Days";
                break;
            case "前3天":
                result = "last3Days";
                break;
            case "前7天":
                result = "last7Days";
                break;
            case "today":
                result = "今天";
                break;
            case "last1Days":
                result = "昨天";
                break;
            case "last2Days":
                result = "前2天";
                break;
            case "last3Days":
                result = "前3天";
                break;
            case "last7Days":
                result = "前7天";
                break;
            default:
                break;
        }
        return result;
    }),
    WEEK("周", "week", (dateValue) -> {
        String result = null;
        switch (dateValue){
            case "本周":
                result = "thisWeek";
                break;
            case "上周":
                result = "lastWeek";
                break;
            case "上周一":
                result = "lastMonday";
                break;
            case "上周二":
                result = "lastTuesday";
                break;
            case "上周三":
                result = "lastWednesday";
                break;
            case "上周四":
                result = "lastThursday";
                break;
            case "上周五":
                result = "lastFriday";
                break;
            case "上周六":
                result = "lastSaturday";
                break;
            case "上周日":
                result = "lastSunday";
                break;
            case "thisWeek":
                result = "本周";
                break;
            case "lastWeek":
                result = "上周";
                break;
            case "lastMonday":
                result = "上周一";
                break;
            case "lastTuesday":
                result = "上周二";
                break;
            case "lastWednesday":
                result = "上周三";
                break;
            case "lastThursday":
                result = "上周四";
                break;
            case "lastFriday":
                result = "上周五";
                break;
            case "lastSaturday":
                result = "上周六";
                break;
            case "lastSunday":
                result = "上周日";
                break;
            default:
                break;
        }
        return result;
    }),
    MONTH("月", "month", (dateValue) -> {
        String result = null;
        switch (dateValue){
            case "本月":
                result = "thisMonth";
                break;
            case "上月":
                result = "lastMonth";
                break;
            case "上月1号":
            case "上月初":
                result = "lastMonthBegin";
                break;
            case "上月最后一天":
            case "上月末":
                result = "lastMonthEnd";
                break;
            case "thisMonth":
                result = "本月";
                break;
            case "lastMonth":
                result = "上月";
                break;
            case "lastMonthBegin":
//                result = "上月初";
                result = "上月1号";
                break;
            case "lastMonthEnd":
//                result = "上月末";
                result = "上月最后一天";
                break;
            default:
                break;
        }
        return result;
    })
    ;


    public static String getDateValue(String cycle, String dateValue){
        return Arrays.stream(TaskCronEnum.values())
                .filter(e -> StringUtils.equals(cycle, e.getCycle()))
                .map(e -> e.dateValue.apply(dateValue))
                .filter(e -> e != null)
                .findFirst()
                .orElse(null);
    }

    public static String getCycleEN(String cycle){
        return Arrays.stream(TaskCronEnum.values())
                .filter(e -> StringUtils.equals(cycle, e.getCycle()))
                .map(e -> e.getCycleEN())
                .filter(e -> e != null)
                .findFirst()
                .orElse(null);
    }

    /**
     * 周期
     */
    private String cycle;
    /**
     * 英文周期
     */
    private String cycleEN;
    /**
     * 周期值
     */
    private Function<String, String> dateValue;

    TaskCronEnum(String cycle, String cycleEN, Function<String, String> dateValue) {
        this.cycle = cycle;
        this.cycleEN = cycleEN;
        this.dateValue = dateValue;
    }
}
