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

@Getter
@AllArgsConstructor
public enum IntegerOptEnum implements EnumInterface {

    /**
     * 大于
     */
    MORE(1, "大于"),

    /**
     * 小于
     */
    LESS(2, "小于"),

    /**
     * 大于等于
     */
    MORE_EQUAL(3, "大于等于"),

    /**
     * 小于等于
     */
    LESS_EQUAL(4, "小于等于"),

    /**
     * 等于
     */
    EQUAL(5, "等于"),
    /**
     * 不等于
     */
    NOT_EQUAL(6, "不等于"),

    /**
     * 介于
     */
    BETWEEN(7, "介于"),

    /**
     * 在列表
     */
    IN(8, "在列表"),

    /**
     * 不在列表
     */
    NOT_IN(9, "不在列表"),
    ;


    private static final Map<Integer, IntegerOptEnum> MAP;

    static {
        MAP = ImmutableMap.copyOf(
                Arrays.stream(IntegerOptEnum.values()).collect(Collectors.toMap(IntegerOptEnum::getValue,
                        Function.identity())));
    }

    private Integer value;

    private String desc;

    public static IntegerOptEnum getByValue(Integer value) {
        return MAP.get(value);
    }

    public static String getSql(String alias, String column, String start, String end, String value,
                                IntegerOptEnum integerOptEnum) {
        StringBuffer sb = new StringBuffer(alias);
        sb.append(".");
        sb.append(column);

        switch (integerOptEnum) {
            case EQUAL:
                sb.append(" = ");
                sb.append("'");
                sb.append(value);
                sb.append("'");
                break;
            case NOT_EQUAL:
                sb.append(" != ");
                sb.append("'");
                sb.append(value);
                sb.append("'");
                break;

            case MORE:
                sb.append(" > ");
                sb.append("'");
                sb.append(value);
                sb.append("'");
                break;
            case MORE_EQUAL:
                sb.append(" >= ");
                sb.append("'");
                sb.append(value);
                sb.append("'");
                break;
            case LESS:
                sb.append(" < ");
                sb.append("'");
                sb.append(value);
                sb.append("'");
                break;
            case LESS_EQUAL:
                sb.append(" <= ");
                sb.append("'");
                sb.append(value);
                sb.append("'");
                break;
            case BETWEEN:
                sb.append(" BETWEEN ");
                sb.append(start);
                sb.append(" AND ");
                sb.append(end);
                break;
            case IN:
                sb.append(" IN ( ");
                sb.append(value);
                sb.append(")");
                break;
            case NOT_IN:
                sb.append(" NOT IN ( ");
                sb.append(value);
                sb.append(")");

                break;

        }
        return sb.toString();
    }


}
