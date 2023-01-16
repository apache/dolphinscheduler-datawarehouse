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

import com.datawarehouse.be.pojo.dto.PythonTypeParams;
import com.datawarehouse.be.pojo.dto.SQLTypeParams;

import java.util.Arrays;

public enum ProcessParamsGenericEnum {
    SQL_PARAMS("SQL", SQLTypeParams.class),
    PYTHON_PARAMS("PYTHON", PythonTypeParams.class);
    /**
     * 入参类型
     */
    private String type;
    /**
     * Params泛型对应的类型
     */
    private Class clazz;

    ProcessParamsGenericEnum(String type, Class clazz) {
        this.type = type;
        this.clazz = clazz;
    }

    /**
     * 获取工作流入参对应的泛型对象
     *
     * @param type
     * @return
     */
    public static Class getProcessParamsClazz(String type) {
        return Arrays.stream(ProcessParamsGenericEnum.values()).filter(e -> e.getType().equals(type)).filter(e -> e != null).findFirst().map(ProcessParamsGenericEnum::getClazz).orElse(null);
    }

    public String getType() {
        return type;
    }

    public Class getClazz() {
        return clazz;
    }
}
