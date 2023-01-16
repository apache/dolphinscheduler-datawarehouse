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
public enum TypeEnum {

    /**
     * 数字
     */
    BIGINT(5, "bigint"),

    /**
     * 数字
     */
    DOUBLE(4, "double"),
    /**
     * 时间
     */
    DATE(3, "date"),


    /**
     * 数字
     */
    NUM(2, "int"),

    /**
     * 字符串
     */
    VAR(1, "string");

    private static final Map<Integer, DateOptEnum> MAP;

    static {
        MAP = ImmutableMap.copyOf(
                Arrays.stream(DateOptEnum.values()).collect(Collectors.toMap(DateOptEnum::getValue,
                        Function.identity())));
    }

    private Integer value;

    private final String desc;


    public static DateOptEnum getByValue(Integer value) {
        return MAP.get(value);
    }


}
