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

package com.datawarehouse.be.utils;

import com.datawarehouse.be.enums.DateOptEnum;
import com.datawarehouse.be.enums.IntegerOptEnum;
import com.datawarehouse.be.enums.JoinTypeEnum;
import com.datawarehouse.be.enums.LogicalOptEnum;
import com.datawarehouse.be.enums.TypeEnum;
import com.datawarehouse.be.enums.VarcharOptEnum;
import com.datawarehouse.be.pojo.dto.ColumnDto;
import com.datawarehouse.be.pojo.dto.ConditionDto;
import com.datawarehouse.be.pojo.dto.ExpressionDto;
import com.datawarehouse.be.pojo.dto.QueryDto;
import com.datawarehouse.be.pojo.dto.TableDto;
import org.apache.dolphinscheduler.dao.entity.User;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Component
public class SqlUtil {

    private static final String CREATE = "CREATE TABLE IF NOT EXISTS";

    private static final String INSERT_OVERWRITE = "INSERT OVERWRITE TABLE ";

    private static final String SELECT = "SELECT \n";

    private static SqlUtil sqlUtil;

    @PostConstruct
    public void init() {
        // 必需
        sqlUtil = this;
    }


    /**
     * 根据传入列明表名设置建表语句
     * @param columnDtoList
     * @param userName
     * @return
     */
    public static String createSql(TableDto targetTable, List<ColumnDto> columnDtoList,String userName) {
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("%s %s.%s", CREATE, targetTable.getDbName(), targetTable.getTableName()));
        sb.append(" ( \n ");
        for (int i = 0; i < columnDtoList.size(); i++) {
            ColumnDto columnDto = columnDtoList.get(i);

            if (columnDto.getComment() == null || columnDto.getComment().length() == 0) {
                sb.append(String.format("%s %s", columnDto.getColumnAlias(), columnDto.getTypeName(), columnDto.getComment()));
            } else {
                sb.append(String.format("%s %s COMMENT '%s'", columnDto.getColumnAlias(), columnDto.getTypeName(), columnDto.getComment().replaceAll(";",","))); }

            if (i == columnDtoList.size() - 1) {
                sb.append(" \n");
            } else {
                sb.append(", \n");
            }
        }

        sb.append(") ");
        if (targetTable.getComment() != null && targetTable.getComment().length() >= 1) {
            sb.append(String.format("COMMENT '%s' \n", targetTable.getComment()));
        }

        sb.append(String.format("PARTITIONED By \n"));
        sb.append(String.format("( dt string COMMENT 'format is yyyyMMdd' ) STORED AS ORC  \n"));
        sb.append(String.format("tblproperties ( 'BUSINESS_OWNER' = '%s', 'DATA_OWNER' = '%s', 'TAGS' = '%s');\n", userName, userName, targetTable.getTableName()));
        return sb.toString();
    }



    //** 对同名列进行处理
    public static void setAliasToSameColumnName(List<ColumnDto> columnDtoList) {
        HashMap<String, Integer> columnMap = new HashMap<>();

        for (ColumnDto columnDto: columnDtoList) {
            if (columnMap.containsKey(columnDto.getColumnName())) {
                columnMap.put(columnDto.getColumnName(),2);
            } else {
                columnMap.put(columnDto.getColumnName(),1);
            }
        }

        for (ColumnDto columnDto: columnDtoList) {
            if (columnMap.get(columnDto.getColumnName()) != 1) {
                columnDto.setColumnAlias(columnDto.getColumnName() + "_of_" + columnDto.getTableName());
            } else {
                columnDto.setColumnAlias(columnDto.getColumnName());
            }
        }
    }
    /**
     * 传入数据生成 SQL
     */
    public static String parseSql(QueryDto queryDto, User user) {

        List<ColumnDto> columnList = queryDto.getColumnList();
        TableDto targetTable = queryDto.getTargetTableDto();

        // 设置列的别名
        setAliasToSameColumnName(columnList);

        // 设置查表的别名
        buildAlias(queryDto);

        String createSQL = createSql(targetTable, columnList, user.getUserName());
        StringBuffer sb = new StringBuffer(createSQL);
        sb.append(" ");

        sb.append(String.format("%s %s.%s partition(dt='$[yyyyMMdd-1]')\n ", INSERT_OVERWRITE,targetTable.getDbName(), targetTable.getTableName()));


        sb.append(SELECT);
        // 拼接查询项
        if (Objects.nonNull(columnList)) {
            for (int i = 0; i < columnList.size(); i++) {
                String alias = getAlias(queryDto.getTableDtoList(), columnList.get(i).getTableName(), columnList.get(i).getDbName());
                sb.append(String.format("%s.%s AS %s", alias, columnList.get(i).getColumnName(), columnList.get(i).getColumnAlias()));

                if (i == columnList.size() - 1) {
                    sb.append(" \n ");
                } else {
                    sb.append(",\n ");
                }

            }
        } else {
            // 请求体为空
        }

        buildSql(sb, queryDto);
        return sb.toString();
    }

    /**
     * 设置别名
     * @param queryDto
     */
    public static void buildAlias(QueryDto queryDto) {
        List<TableDto> tableDtos = queryDto.getTableDtoList();

        final int[] i = {1};

        tableDtos.forEach( tableDto -> {
            tableDto.setAlias("tab" + (i[0]++));
        });

    }


    /**
     * 获取别名
     * @param tableDtos
     * @param tableName
     * @param dbName
     * @return
     */
    public static String getAlias(List<TableDto> tableDtos, String tableName, String dbName) {
        final String[] alias = {null};
        tableDtos.stream()
                .filter(tableDto -> {
                    return tableName.equals(tableDto.getTableName()) && dbName.equals(tableDto.getDbName());
                })
                .forEach(tableDto -> {
                    alias[0] = tableDto.getAlias();
                    return;
                });
        return alias[0];
    }


    /**
     * 建立 sql
     * @param sb
     * @param queryDto
     */
    public static void buildSql(StringBuffer sb,QueryDto queryDto) {
        sb.append(" FROM \n");
        List<TableDto> tableDtos = queryDto.getTableDtoList();
        final  int[] i = {0};

        Map<String,String> dtMap = new HashMap<>();
        List<ConditionDto> conditionDtoList = queryDto.getConditionDtoList();
        for (ConditionDto conditionDto : conditionDtoList) {
            List<ConditionDto> subConditionDtoList = conditionDto.getSubConditions();
            for (ConditionDto subCondition : subConditionDtoList) {
                ExpressionDto expressionDto = subCondition.getExpression();
                if (expressionDto.getColumnName().equals("dt")) {
                    dtMap.put(expressionDto.getDbName() + "." + expressionDto.getTableName(), expressionDto.getValue());
                }
            }
        }


        tableDtos.forEach(tableDto -> {
            String tableName = tableDto.getDbName() + "." + tableDto.getTableName();
            if (i[0] == 0) {
                sb.append(String.format(" (SELECT * FROM %s WHERE dt='%s' ) as %s",
                        tableName,
                        dtMap.get(tableName),
                        tableDto.getAlias()));

            } else if (i[0] == tableDtos.size()) {

            } else {

                JoinTypeEnum joinTypeEnum = JoinTypeEnum.getByValue(tableDto.getRelationDto().getJoin());
                sb.append(JoinTypeEnum.getSql(tableDtos, tableDto, joinTypeEnum,dtMap.get(tableName)));
            }
            i[0]++;
        });
        if (queryDto.getConditionDtoList() != null && queryDto.getConditionDtoList().size() != 0) {
            sb.append(" WHERE \n");
            //拼接查询条件
            StringBuffer tempSb = new StringBuffer();
            sb.append(getConditionSql(queryDto, queryDto.getConditionDtoList(), tempSb));
        }
    }


    /**
     * 添加参数条件
     * @param queryDto
     * @param conditionDtoList
     * @param sb
     * @return
     */
    public static String getConditionSql(QueryDto queryDto, List<ConditionDto> conditionDtoList, StringBuffer sb) {
        for (int i = 0; i < conditionDtoList.size(); i++) {
            ConditionDto conditionDto = conditionDtoList.get(i);
            if (i == 0) {
                conditionDto.setOption(null);
            }
            if (Objects.isNull(conditionDto.getSubConditions()) || conditionDto.getSubConditions().size() == 0) {
                if (conditionDto.getOption() != null && conditionDto.getOption() != 0) {
                    sb.append(LogicalOptEnum.getSql(LogicalOptEnum.getByValue(conditionDto.getOption())));
                } else {
                    sb.append(" ( ");
                }
                //TODO 设置表达式
                ExpressionDto expressionDto = conditionDto.getExpression();
                if (TypeEnum.VAR.getDesc().equals(expressionDto.getTypeName())) {

                    String value = "";
                    sb.append(VarcharOptEnum.getSql(getAlias(queryDto.getTableDtoList(), expressionDto.getTableName(), expressionDto.getDbName()), expressionDto.getColumnName(), expressionDto.getStart(), expressionDto.getEnd(), "".equals(value) ? expressionDto.getValue() : value, VarcharOptEnum.getByValue(expressionDto.getExpressionType())));
                } else if (TypeEnum.NUM.getDesc().equals(expressionDto.getTypeName()) || TypeEnum.BIGINT.getDesc().equals(expressionDto.getTypeName()) || TypeEnum.DOUBLE.getDesc().equals(expressionDto.getTypeName())) {

                    String value = "";
                    sb.append(IntegerOptEnum.getSql(getAlias(queryDto.getTableDtoList(), expressionDto.getTableName(), expressionDto.getDbName()), expressionDto.getColumnName(), expressionDto.getStart(), expressionDto.getEnd(), "".equals(value) ? expressionDto.getValue() : value, IntegerOptEnum.getByValue(expressionDto.getExpressionType())));

                } else if (TypeEnum.DATE.getDesc().equals(expressionDto.getTypeName())) {
                    sb.append(DateOptEnum.getSql(getAlias(queryDto.getTableDtoList(), expressionDto.getTableName(), expressionDto.getDbName()), expressionDto.getColumnName(), expressionDto.getStart(), expressionDto.getEnd(), expressionDto.getValue(), DateOptEnum.getByValue(expressionDto.getExpressionType())));
                }
            } else {
                if (conditionDto.getOption() != null && conditionDto.getOption() != 0) {
                    {
                        sb.append(LogicalOptEnum.getSql(LogicalOptEnum.getByValue(conditionDto.getOption())));
                    }
                }
                getConditionSql(queryDto, conditionDto.getSubConditions(), sb);
                sb.append(" ) ");
            }
        }
        return sb.toString();
    }


}
