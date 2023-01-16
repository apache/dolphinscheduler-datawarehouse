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

package com.datawarehouse.be.service.impl;

import com.google.common.collect.Lists;
import com.datawarehouse.be.dao.hive.HiveSqlOperationMapper;
import com.datawarehouse.be.pojo.dto.ColumnDto;
import com.datawarehouse.be.pojo.dto.QueryDto;
import com.datawarehouse.be.pojo.params.SqlParsingParam;
import com.datawarehouse.be.pojo.vo.DataResultVO;
import com.datawarehouse.be.pojo.vo.HeaderVO;
import com.datawarehouse.be.service.HiveSqlOperationService;
import com.datawarehouse.be.service.ProcessService;
import com.datawarehouse.be.utils.SqlUtil;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.exceptions.ServiceException;
import org.apache.dolphinscheduler.api.service.impl.BaseServiceImpl;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service("hiveSqlOperationService")
public class HiveSqlOperationServiceImpl extends BaseServiceImpl implements HiveSqlOperationService {

    private static final Logger logger = LoggerFactory.getLogger(HiveMetaServiceImpl.class);

    @Autowired
    HiveSqlOperationMapper hiveSqlOperationMapper;

    @Autowired
    ProcessService processService;

    private String regex = "\\$\\[yyyyMMdd-([\\d])*\\]";

    @Override
    public Map<String, Object> parseSql(QueryDto queryDto, User user) {
        Map<String, Object> result = new HashMap<>();
        String sql = SqlUtil.parseSql(queryDto, user);
        putMsg(result, Status.SUCCESS);
        result.put(Constants.DATA_LIST, sql);
        return result;
    }

    @Override
    public Map<String, Object> getDemoDataBySQL(SqlParsingParam sqlParsingParam, User user) throws Exception {
        Map<String, Object> result = new HashMap<>();
        String sql = sqlParsingParam.getSql();
        processService.sqlParsing(sqlParsingParam, user);
        String replaceEnterSql = sql.replaceAll("\\\\n", "\n");
        String parseSql = replaceDtAndGetSelectSql(replaceEnterSql);
        List<ColumnDto> columnDtoList = ((PlainSelect) ((Select) CCJSqlParserUtil.parse(parseSql)).getSelectBody()).getSelectItems()
                .stream()
                .map(item -> (SelectExpressionItem) item)
                .map(item -> {
                    if (Objects.nonNull(item.getAlias())) {
                        return item.getAlias().getName();
                    }
                    if (item.getExpression() instanceof Column) {
                        return ((Column) item.getExpression()).getColumnName();
                    }
                    throw new RuntimeException("字段:" + item.toString() + "无法解析到别名，请确认sql正确");
                })
                .map(item->item.toLowerCase())
                .map(item -> item.replaceAll("`", ""))
                .map(item -> ColumnDto.builder().columnName(item).comment(item).build())
                .collect(Collectors.toList());
        DataResultVO dataResultVO = queryBySql(columnDtoList, parseSql);
        putMsg(result, Status.SUCCESS);
        result.put(Constants.DATA_LIST, dataResultVO);
        return result;
    }

    private DataResultVO queryBySql(List<ColumnDto> columnDtoList, String sql)  {
        try {
            hiveSqlOperationMapper.explain(sql);
            System.out.println(sql);
        } catch (Exception e) {
            logger.error("sql: {}, 不可执行,错误原因:{}", sql,e);
            throw new ServiceException("SQL 有误，请确认 SQL 书写正确");
        }
        // 构建 headerVOList
        List<HeaderVO> headerVOList = Lists.newArrayList();
        columnDtoList.forEach(columnDto -> {
            HeaderVO headerVO = HeaderVO.builder()
                    .key(columnDto.getColumnName())
                    .key(columnDto.getComment())
                    .build();
            headerVOList.add(headerVO);
        });
        // 构建返回结果集合
        List<Map<String, Object>> dataResultList = hiveSqlOperationMapper.getDemoDataBySQL(sql);

        DataResultVO dataResultVO = DataResultVO.builder()
                .dataResultList(dataResultList)
                .headerVOList(headerVOList)
                .build();
        return dataResultVO;
    }

    private String replaceDtAndGetSelectSql(String sql) {
        String parseSql;
        if (sql.contains("select")) {
            parseSql = sql.split("select", 2)[1];
        } else if (sql.contains("SELECT")) {
            parseSql = sql.split("SELECT", 2)[1];
        } else {
            logger.error("sql error {}", sql);
            return  null;
        }
        parseSql = "SELECT " + parseSql;
        Set<Integer> days = new HashSet<>();
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(parseSql);
        while (matcher.find()) {
            days.add(Integer.parseInt(matcher.group(1)));
        }
        for (Integer day : days) {
            parseSql = parseSql.replaceAll("\\$\\[yyyyMMdd-" + day.toString() +"\\]", LocalDate.now().minusDays(day).format(DateTimeFormatter.ofPattern("yyyyMMdd")));
        }
        logger.info("替换分区函数后的 sql 语句\n {}：", parseSql);
        return parseSql;
    }
}
