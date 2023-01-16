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
public enum LogicalOptEnum implements EnumInterface {
    /**
     * 求和
     */
    AND(1, "且", "_and "),

    OR(2, "或", "_or ");

    private static final Map<Integer, LogicalOptEnum> MAP;

    static {
        MAP = ImmutableMap.copyOf(
                Arrays.stream(LogicalOptEnum.values()).collect(Collectors.toMap(LogicalOptEnum::getValue,
                        Function.identity())));
    }

    private Integer value;

    private String desc;

    private String lable;

    public static LogicalOptEnum getByValue(Integer value) {
        return MAP.get(value);
    }

    public static String getSql(LogicalOptEnum logicalOptEnum) {
        StringBuffer sb = new StringBuffer();
        switch (logicalOptEnum) {
            case AND:
                sb.append(" AND ");

                break;
            case OR:
                sb.append(" OR ");

                break;

            default:
                break;
        }

        return sb.toString();
    }
}
