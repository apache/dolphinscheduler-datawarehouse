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
import com.datawarehouse.be.pojo.dto.OnConditionDto;
import com.datawarehouse.be.pojo.dto.TableDto;
import com.datawarehouse.be.utils.SqlUtil;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@AllArgsConstructor
@Getter
public enum JoinTypeEnum implements EnumInterface {

    /**
     * 左外关联
     */
    LEFT(1, "左连接", "_left "),

    RIGHT(2, "右连接", "_right "),

    INNER(3, "内连接", "_inner "),

    FULL(4, "全外链接 ", "_full ");


    private static final Map<Integer, JoinTypeEnum> MAP;

    static {
        MAP = ImmutableMap.copyOf(
                Arrays.stream(JoinTypeEnum.values()).collect(Collectors.toMap(JoinTypeEnum::getValue,
                        Function.identity())));
    }

    private Integer value;

    private String desc;

    private String lable;

    public static JoinTypeEnum getByValue(Integer value) {
        return MAP.get(value);
    }

    public static String getSql(List<TableDto> tableDtos, TableDto tableDto, JoinTypeEnum joinTypeEnum,String dt) {
        StringBuffer sb = new StringBuffer();
        switch (joinTypeEnum) {
            case LEFT:
                sb.append(" LEFT JOIN ");
                break;
            case RIGHT:
                sb.append(" RIGHT JOIN ");

                break;
            case INNER:
                sb.append(" INNER JOIN ");

                break;
            case FULL:
                sb.append(" FULL JOIN ");

                break;

            default:
        }
        String tableName = tableDto.getDbName() + "." + tableDto.getTableName();
        sb.append(String.format(" (SELECT * FROM %s WHERE dt='%s' ) as %s",
                tableName,
                dt,
                tableDto.getAlias()));
        sb.append(" ON ");
        List<OnConditionDto> onConditionDtoList = tableDto.getRelationDto().getOnList();

        for (int i = 0; i < onConditionDtoList.size(); i++) {
            String leftAlias = SqlUtil.getAlias(tableDtos, onConditionDtoList.get(i).getLeft().getTableName(), onConditionDtoList.get(i).getLeft().getDbName());
            String rightAlias = SqlUtil.getAlias(tableDtos, onConditionDtoList.get(i).getRight().getTableName(), onConditionDtoList.get(i).getRight().getDbName());
            sb.append(leftAlias);
            sb.append(".");
            sb.append(onConditionDtoList.get(i).getLeft().getColumnName());
            sb.append(" = ");
            sb.append(rightAlias);
            sb.append(".");
            sb.append(onConditionDtoList.get(i).getRight().getColumnName());
            if (i == onConditionDtoList.size() - 1) {
                sb.append(" ");
            } else {
                sb.append(" AND ");
            }
        }
        return sb.toString();
    }

}
