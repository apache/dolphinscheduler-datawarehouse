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

import com.google.common.collect.ImmutableMap;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
public enum VarcharOptEnum implements EnumInterface {

    /**
     * 等于
     */
    EQUAL(5, "等于"),
    /**
     * 不等于
     */
    NOT_EQUAL(6, "不等于"),

    /**
     * 在列表
     */
    IN(8, "在列表"),

    /**
     * 不在列表
     */
    NOT_IN(9, "不在列表"),

    /**
     * 包含
     */
    LIKE(10, "包含(like)"),

    /**
     * 不包含
     */
    NOT_LIKE(11, "不包含(not like)"),
    /**
     * 等于空
     */
    IS_NULL(12, "等于空"),

    /**
     * 不等于空
     */
    IS_NOT_NULL(13, "不等于空");


    private static final Map<Integer, VarcharOptEnum> MAP;

    static {
        MAP = ImmutableMap.copyOf(
                Arrays.stream(VarcharOptEnum.values()).collect(Collectors.toMap(VarcharOptEnum::getValue,
                        Function.identity())));
    }

    private Integer value;

    private String desc;

    public static VarcharOptEnum getByValue(Integer value) {
        return MAP.get(value);
    }

    /**
     * 获取SQL语句片段
     *
     * @param alias          表别名
     * @param column         条件列
     * @param start          开始
     * @param end            结束
     * @param varcharOptEnum 时间操作类型
     * @return sql 片段
     */
    public static String getSql(String alias, String column, String start, String end, String value, VarcharOptEnum varcharOptEnum) {
        String field = String.format("%s.%s", alias, column);
        StringBuffer sb = new StringBuffer();

        switch (varcharOptEnum) {
            case LIKE:
                sb.append(field);
                sb.append(" LIKE ");
                sb.append("'%");
                sb.append(value);
                sb.append("%'");
                break;
            case NOT_LIKE:
                sb.append(field);
                sb.append(" NOT LIKE ");
                sb.append("'%");
                sb.append(value);
                sb.append("%'");
                break;
            case NOT_EQUAL:
                sb.append(field);
                sb.append(" != ");
                sb.append("'");
                sb.append(value);
                sb.append("'");
                break;
            case EQUAL:
                sb.append(field);
                sb.append(" = ");
                sb.append("'");
                sb.append(value);
                sb.append("'");
                break;
            case IN:
                sb.append(field);
                sb.append(" IN ( ");
                sb.append(value);
                sb.append(")");
                break;
            case NOT_IN:
                sb.append(field);
                sb.append(" NOT IN ( ");
                sb.append(value);
                sb.append(")");
                break;
            case IS_NULL:
                sb.append("(");
                sb.append(field);
                sb.append(" IS NULL OR ");
                sb.append(field);
                sb.append(" = '' )");

                break;
            case IS_NOT_NULL:
                sb.append("(");
                sb.append(field);
                sb.append(" IS NOT NULL AND ");

                sb.append(field);
                sb.append(" != '')");
                break;
        }
        return sb.toString();
    }
}
