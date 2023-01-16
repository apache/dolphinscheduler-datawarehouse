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

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.datawarehouse.be.exceptions.ProcessParamsException;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Map;

public enum ProcessCronEnum {
    TODAY("每日", null),
    WEEKLY("每周", new String[]{"周一", "周二", "周三", "周四", "周五", "周六", "周日"}),
    MONTHLY("每月", new String[]{"第一天", "最后一天", "1号", "2号", "3号", "4号", "5号"});

    /**
     * 获取周几对应下标
     *
     * @param dateValue
     * @return
     */
    public static int getDayOfWeek(String dateValue) {
        switch (dateValue) {
            case "周一":
                return 2;
            case "周二":
                return 3;
            case "周三":
                return 4;
            case "周四":
                return 5;
            case "周五":
                return 6;
            case "周六":
                return 7;
            case "周日":
                return 1;
            default:
                // 异常
                throw new ProcessParamsException(String.format("入参每周值不合法，请确认在[%s]范围内~", JSON.toJSONString(WEEKLY.getDateValue())));
        }
    }

    /**
     * 获取周几对应下标
     *
     * @param dateValue
     * @return
     */
    public static String getDayOfMonth(String dateValue) {
        switch (dateValue) {
            case "第一天":
            case "1号":
                return "1";
            case "最后一天":
                return "L";
            case "2号":
                return "2";
            case "3号":
                return "3";
            case "4号":
                return "4";
            case "5号":
                return "5";
            default:
                // 异常
                throw new ProcessParamsException(String.format("入参日期值不合法，请确认在%s范围内~", JSON.toJSONString(MONTHLY.getDateValue())));
        }
    }

    /**
     * 转换成map
     *
     * @return 数据源类型map
     */
    public static Map<String, String[]> toMap() {
        Map<String, String[]> map = Maps.newHashMap();
        for (ProcessCronEnum item : ProcessCronEnum.values()) {
            map.put(item.getCycle(), item.getDateValue());
        }
        return map;
    }

    public static ProcessCronEnum get(String cycle) {
        return Arrays.stream(ProcessCronEnum.values()).filter(e -> cycle.equals(e.getCycle())).filter(e -> e != null).findFirst().orElse(null);
    }

    public static void main(String[] args) {
        System.out.println(isExistsOfDateValue("月", "最后一天"));
    }

    public static boolean isExistsOfDateValue(String cycle, String dataValue) {
        return Arrays.stream(ProcessCronEnum.values())
                .filter(e -> StringUtils.equals(cycle, e.getCycle()))
                .filter(a -> a.getDateValue() != null)
                .map(e -> Arrays.stream(e.getDateValue()).anyMatch(i -> dataValue.equals(i)))
                .filter(e -> e != null)
                .findFirst()
                .orElse(false);
    }

    public static boolean isExists(String cron) {
        if (StringUtils.isBlank(cron)) {
            return false;
        }
        String[] cronArray = cron.split("\\.");
        if (cronArray.length > 2) {
            return false;
        }
        String level1 = cronArray[0];
        String level2 = cronArray.length == 2 ? cronArray[1] : null;
        return Arrays.stream(ProcessCronEnum.values()).anyMatch(e -> {
            boolean level1Flag = StringUtils.equals(e.getCycle(), level1);
            if (level2 == null) {
                return level1Flag;
            } else {
                return level1Flag && Arrays.stream(e.getDateValue()).anyMatch(i -> StringUtils.equals(i, level2));
            }
        });
    }

    /**
     * 周期
     */
    private String cycle;
    /**
     * 周期值
     */
    private String[] dateValue;

    ProcessCronEnum(String cycle, String[] dateValue) {
        this.cycle = cycle;
        this.dateValue = dateValue;
    }

    public String getCycle() {
        return cycle;
    }

    public String[] getDateValue() {
        return dateValue;
    }
}
