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

package com.datawarehouse.be.dao.hivemeta;

import com.datawarehouse.be.pojo.dos.HiveTabParamInfoDO;
import com.datawarehouse.be.pojo.dto.ColumnDto;
import com.datawarehouse.be.pojo.vo.HiveDbTabVO;
import com.datawarehouse.be.pojo.vo.HiveTabPartitionVO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Result;
import org.apache.ibatis.annotations.Results;
import org.apache.ibatis.annotations.Select;

import java.util.LinkedList;
import java.util.Map;

@Mapper
public interface HiveMetaMapper {

    @Select("select t.TBL_NAME\n" +
            "from dbs  d\n" +
            "join tbls t on d.DB_ID = t.DB_ID\n" +
            "where NAME=#{dbName};")
    LinkedList<String> getHiveTabsByDb(String dbName);


    @Select("select d.NAME as dbName,\n" +
            "        t.TBL_NAME as tabName\n" +
            "from dbs  d\n" +
            "join tbls t on d.DB_ID = t.DB_ID\n" +
            "where\n" +
            "      NAME like CONCAT('%', #{keyword}, '%') \n" +
            "    OR  t.TBL_NAME like CONCAT('%', #{keyword}, '%') ")
    LinkedList<HiveDbTabVO> getHiveTabsByKeyword(String keyword);


    @Select("select PARAM_KEY, PARAM_VALUE\n" +
            "from table_params params\n" +
            "    join tbls t on params.TBL_ID = t.TBL_ID\n" +
            "    join dbs d on t.DB_ID = d.DB_ID\n" +
            "where t.TBL_NAME = #{tabName} \n" +
            "  and d.NAME = #{dbName}")
    LinkedList<HiveTabParamInfoDO> getHiveTabParamsInfo(@Param("dbName") String dbName, @Param("tabName") String tabName);

    @Select("select\n" +
            "       pk.PKEY_NAME,\n" +
            "       pk.PKEY_COMMENT\n" +
            "from tbls t\n" +
            "join partition_keys pk on t.TBL_ID = pk.TBL_ID\n" +
            "join dbs d on t.DB_ID = d.DB_ID\n" +
            "where TBL_NAME = #{tabName}\n" +
            "and d.NAME = #{dbName}\n" +
            "group by t.TBL_NAME, d.NAME,PKEY_NAME,PKEY_COMMENT ")
    @Results({
            @Result(column = "PKEY_COMMENT", property = "pkComment"),
            @Result(column = "PKEY_NAME", property = "pkName")
    })
    LinkedList<HiveTabPartitionVO> getPartitionNames(@Param("dbName") String dbName, @Param("tabName") String tabName);


    @Select("select\n" +
            "       c.COLUMN_NAME as COlUMN_NAME,\n" +
            "       c.TYPE_NAME as TYPE_NAME,\n" +
            "       c.COMMENT as COMMENT,\n" +
            "       t.TBL_NAME as tableName,\n" +
            "       d.NAME as dbName\n " +
            "from (\n" +
            "    tbls as t join dbs as d on t.DB_ID = d.DB_ID)\n" +
            "    join sds as s on s.SD_ID=t.SD_ID\n" +
            "    join columns_v2 c on s.CD_ID = c.CD_ID\n" +
            "where t.TBL_NAME = #{tabName} and d.NAME = #{dbName}")
    @Results({
            @Result(column = "COLUMN_NAME", property = "columnName"),
            @Result(column = "TYPE_NAME", property = "typeName"),
            @Result(column = "COMMENT", property = "comment"),
            @Result(column = "tableName", property = "tableName"),
            @Result(column = "dbName", property = "dbName")
    })
    LinkedList<ColumnDto> getTableColumnList(@Param("dbName") String dbName, @Param("tabName") String tabName);


    @Select("select count(TBL_NAME) as isExsits\n" +
            "from dbs  d\n" +
            "join tbls t on d.DB_ID = t.DB_ID\n" +
            "where t.TBL_NAME=#{tabName} and d.NAME = #{dbName};")
    Integer isTheTableExsits(@Param("dbName")String dbName, @Param("tabName") String tabName);



    /**
     * export data use it to get all dt table
     * @param dbName
     * @return
     */
    @Select({"<script>"+
            "select TBL_NAME\n" +
            "from partition_keys\n" +
            "inner join tbls t on partition_keys.TBL_ID = t.TBL_ID\n" +
            "inner join dbs d on t.DB_ID = d.DB_ID\n" +
            "where 1=1\n"+
            "<if test='dbName!=null'> and d.NAME = #{dbName}</if>"
            +"</script>"})
    LinkedList<String> getDTHiveTable(@Param("dbName") String dbName);

    @Select("select d.Name as dbName,t.TBL_NAME as tabName\n" +
            "from tbls t\n" +
            "join dbs d on d.DB_ID = t.DB_ID\n" +
            "join partition_keys pk on t.TBL_ID = pk.TBL_ID\n"+
            "where\n" +
            "pk.PKEY_NAME='dt' and (d.NAME LIKE CONCAT('%',#{keyword},'%') OR t.TBL_NAME LIKE CONCAT('%',#{keyword},'%'));")
    LinkedList<HiveDbTabVO> getTabsByFuzzyQuery(String keyword);


    /**
     * 获取单个分区表的最新分区
     * @param dbName
     * @param tabName
     * @return
     */
    @Select("select\n" +
            "       PKEY_NAME,\n" +
            "       SUBSTRING_INDEX(PART_NAME, concat(PKEY_NAME, '='), -1) as PART_DT\n" +
            "from partition_keys\n" +
            "inner join tbls t on partition_keys.TBL_ID = t.TBL_ID\n" +
            "inner join dbs d on t.DB_ID = d.DB_ID\n" +
            "inner join partitions p on partition_keys.TBL_ID = p.TBL_ID\n" +
            "where TBL_NAME = #{tabName} and d.NAME = #{dbName}\n" +
            "order by PART_NAME desc limit 1;")
    LinkedList<Map<String,String>> getLastestPartition(@Param("dbName") String dbName, @Param("tabName") String tabName);


    @Select("SELECT tp.PARAM_VALUE\n" +
            "from tbls\n" +
            "join dbs on dbs.DB_ID = tbls.DB_ID\n" +
            "join table_params tp on tp.TBL_ID = tbls.TBL_ID\n" +
            "where tp.PARAM_KEY = 'DATA_OWNER' and dbs.NAME = #{dbName} and tbls.TBL_NAME=#{tableName};")
    String getTableOwner(@Param("dbName") String dbName, @Param("tableName") String tableName);

    /**
     * 获取单个分区表的最新分区
     * @param dbName
     * @param tabName
     * @return
     */
    @Select("select\n" +
            "       PKEY_NAME,\n" +
            "       SUBSTRING_INDEX(PART_NAME, concat(PKEY_NAME, '='), -1) as PART_DT\n" +
            "from partition_keys\n" +
            "inner join tbls t on partition_keys.TBL_ID = t.TBL_ID\n" +
            "inner join dbs d on t.DB_ID = d.DB_ID\n" +
            "inner join partitions p on partition_keys.TBL_ID = p.TBL_ID\n" +
            "where TBL_NAME = #{tabName} and d.NAME = #{dbName}\n" +
            "order by PART_NAME desc limit 1;")
    LinkedList<Map<String,String>> getLatestPartition(@Param("dbName") String dbName, @Param("tabName") String tabName);

}
