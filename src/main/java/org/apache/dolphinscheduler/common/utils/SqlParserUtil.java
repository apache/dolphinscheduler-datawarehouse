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

package org.apache.dolphinscheduler.common.utils;

import com.google.common.collect.Maps;
import com.datawarehouse.be.utils.LineageInfo;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.dolphinscheduler.common.exception.SqlParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlParserUtil {

    private static final Logger logger = LoggerFactory.getLogger(SqlParserUtil.class);

    /**
     * 输入表标识
     */
    public final static String INPUT = "input";
    /**
     * 输出表标识
     */
    public final static String OUT_PUT = "output";
    /**
     * 输出表标识
     */
    public final static String WITH_TABLE = "withTable";

    public static void main(String[] args) {
        String sql = "INSERT OVERWRITE TABLE bl_iao.dwd_data_governance_property_f_d partition (dt = '$[yyyyMMdd-1]')\n" +
                "select *\n" +
                "from (with property as (\n" +
                "    select p.*\n" +
                "    from (select *\n" +
                "          from dwd.dwd_cosmos_core_resblock_property_f_d\n" +
                "          where dt = date_format(date_add(CURRENT_DATE(), -1), 'YYYYMMdd')\n" +
                "            and status = 1) p\n" +
                "             inner join\n" +
                "\n" +
                "         (select resblock_id\n" +
                "          from dwd.dwd_cosmos_core_resblock_f_d\n" +
                "          where dt = date_format(date_add(CURRENT_DATE(), -1), 'YYYYMMdd')\n" +
                "            and status = 1) r on p.resblock_id = r.resblock_id\n" +
                ")\n" +
                "\n" +
                "/* 1.物业服务类型 property_type\n" +
                "    1、1:物业公司 , 2:房管所代管物业 , 3:街道/社区代管物业 , 4:单位自/代管物业, 5:业主自管 */\n" +
                "      select '楼盘-物业'                   as type,\n" +
                "             'property_type'           as field,\n" +
                "             '物业服务类型'                  as field_name,\n" +
                "             t1.city_code,\n" +
                "             t1.resblock_id,\n" +
                "             t1.id                     as id,\n" +
                "             t1.property_type          as value,\n" +
                "             concat_ws(\"|\", t1.rule_1) as hit_rule,\n" +
                "             ''                        as field_level,\n" +
                "             ''                        as is_white_list\n" +
                "      from (select resblock_id,\n" +
                "                   id,\n" +
                "                   city_code,\n" +
                "                   property_type,\n" +
                "                   (case\n" +
                "                        when property_type is not null and property_type not in (1, 2, 3, 4, 5)\n" +
                "                            then '规则1:不符合1物业公司,2房管所代管物业,3街道/社区代管物业,4单位自/代管物业,5业主自管'\n" +
                "                       end) as rule_1\n" +
                "            from property) t1\n" +
                "      where rule_1 is not null\n" +
                "\n" +
                "      union\n" +
                "\n" +
                "/* 1.物业名称 name\n" +
                "    1、字符串长度≥2\n" +
                "    2、符号可有：中英文括号、连接符-，不可有：不可有除以上列举之外的特殊符号，无表情符号\n" +
                "    3、不允许纯数字或纯英文\n" +
                "    4、特殊规则：【物业名称】表述不可为【无物业管理服务/无/无物业/没有/没/没物业/无物业管理公司/无物业管理/暂时无物业公司/暂无物业管理公司/无声/无名/无锡/不知道/不知/不详/暂无资料/不清楚/不明/暂信息/物业/物业一/与物业/全部】\n" +
                "    5、同小区内不允许与已有物业公司同名\n" +
                "    6、同小区的有效物业数量≤5个\n" +
                "*/\n" +
                "      select '楼盘物业'                    as type,\n" +
                "             'name'                    as field,\n" +
                "             '物业名称'                    as field_name,\n" +
                "             t1.city_code,\n" +
                "             t1.resblock_id,\n" +
                "             t1.id                     as id,\n" +
                "             t1.name                   as value,\n" +
                "             concat_ws(\"|\", t1.rule_1) as hit_rule,\n" +
                "             ''                        as field_level,\n" +
                "             ''                        as is_white_list\n" +
                "      from (select resblock_id,\n" +
                "                   id,\n" +
                "                   city_code,\n" +
                "                   name,\n" +
                "                   (case\n" +
                "                        when name is not null and length(name) < 2 then '规则1:字符串长度≥2'\n" +
                "                        when name regexp '[`~_!@#$%^&*+=|{}:;,\\\\[\\\\].<>/?~！@#￥%……&*——+|{}【】‘；：”“’。，、？]'\n" +
                "                            then '规则2:符号可有：中英文括号、连接符-,不可有：不可有除以上列举之外的特殊符号，无表情符号'\n" +
                "                        when name regexp '^[0-9]+$' or name regexp '^[a-zA-Z]+$' then '规则3:不允许纯数字或纯英文'\n" +
                "                        when name in\n" +
                "                             ('无物业管理服务', '无', '无物业', '没有', '没', '没物业', '无物业管理公司', '无物业管理', '暂时无物业公司',\n" +
                "                              '暂无物业管理公司', '无声', '无名',\n" +
                "                              '无锡', '不知道', '不知', '不详', '暂无资料', '不清楚', '不明', '暂信息', '物业', '物业一', '与物业', '全部')\n" +
                "                            then '规则4:【物业名称】表述不可为【无物业管理服务/无/无物业/没有/没/没物业/无物业管理公司/无物业管理/暂时无物业公司/暂无物业管理公司/无声/无名/无锡/不知道/不知/不详/暂无资料/不清楚/不明/暂信息/物业/物业一/与物业/全部】'\n" +
                "                       end) as rule_1\n" +
                "            from property\n" +
                "\n" +
                "            union all\n" +
                "\n" +
                "            select temp.resblock_id,\n" +
                "                   tt.id,\n" +
                "                   tt.city_code,\n" +
                "                   tt.name,\n" +
                "                   '规则5：同小区内不允许与已有物业公司同名' as rule_1\n" +
                "            from (select p.resblock_id,\n" +
                "                         p.id,\n" +
                "                         p.city_code,\n" +
                "                         p.name\n" +
                "                  from property p) tt\n" +
                "\n" +
                "                     right join\n" +
                "\n" +
                "                 (select resblock_id, name, count(1) cnt\n" +
                "                  from property\n" +
                "                  group by resblock_id, name\n" +
                "                  having cnt > 1) temp on tt.resblock_id = temp.resblock_id and tt.name = temp.name\n" +
                "\n" +
                "            union all\n" +
                "\n" +
                "            select temp.resblock_id,\n" +
                "                   tt.id,\n" +
                "                   tt.city_code,\n" +
                "                   tt.name,\n" +
                "                   '规则6：同小区内不允许与已有物业公司同名' as rule_1\n" +
                "            from (select p.resblock_id,\n" +
                "                         p.id,\n" +
                "                         p.city_code,\n" +
                "                         p.name\n" +
                "                  from property p) tt\n" +
                "\n" +
                "                     right join\n" +
                "\n" +
                "                 (select resblock_id, count(1) cnt\n" +
                "                  from property\n" +
                "                  group by resblock_id\n" +
                "                  having cnt > 5) temp on tt.resblock_id = temp.resblock_id\n" +
                "           ) t1\n" +
                "\n" +
                "      where rule_1 is not null\n" +
                "\n" +
                "      union\n" +
                "\n" +
                "/* 1.物业办公地址 address\n" +
                "    1、字符串长度≥2\n" +
                "    2、符号可有：中英文括号，连接符-，井号#，下划线\n" +
                "    3、不可有：不允许含有除以上列举之外的特殊符号，无表情符号\n" +
                "    4、不允许纯数字或纯英文\n" +
                "    5、【物业地址】表述不可为【小区内/小区内部/小区/小区里面/小区里/物业/本小区/小区附近/小区外面/小区院内/小区门口/小区进门/门口】\n" +
                " */\n" +
                "      select '楼盘物业'                    as type,\n" +
                "             'address'                 as field,\n" +
                "             '物业办公地址'                  as field_name,\n" +
                "             t1.city_code,\n" +
                "             t1.resblock_id,\n" +
                "             t1.id                     as id,\n" +
                "             t1.address                as value,\n" +
                "             concat_ws(\"|\", t1.rule_1) as hit_rule,\n" +
                "             ''                        as field_level,\n" +
                "             ''                        as is_white_list\n" +
                "      from (select resblock_id,\n" +
                "                   id,\n" +
                "                   city_code,\n" +
                "                   address,\n" +
                "                   length(address),\n" +
                "                   (case\n" +
                "                        when length(address) > 0 and length(address) < 2 then '规则1:字符串长度≥2'\n" +
                "                        when address regexp '[`~!@$%^&*+=|{}:;,\\\\[\\\\].<>/?~！@￥%……&*——+|{}【】‘；：”“’。，、？]'\n" +
                "                            then '规则2:符号可有：中英文括号，连接符-，井号#，下划线,不可有：不可有除以上列举之外的特殊符号，无表情符号'\n" +
                "                        when name regexp '^[0-9]+$' or name regexp '^[a-zA-Z]+$' then '规则3:不允许纯数字或纯英文'\n" +
                "                        when name in\n" +
                "                             ('小区内', '小区内部', '小区', '小区里面', '小区里', '物业', '本小区', '小区附近', '小区外面', '小区院内', '小区门口',\n" +
                "                              '小区进门', '门口')\n" +
                "                            then '规则4:【物业地址】表述不可为【小区内/小区内部/小区/小区里面/小区里/物业/本小区/小区附近/小区外面/小区院内/小区门口/小区进门/门口】'\n" +
                "                       end) as rule_1\n" +
                "            from property\n" +
                "           ) t1\n" +
                "      where rule_1 is not null\n" +
                "\n" +
                "      union all\n" +
                "\n" +
                "/* 物业电话 phone\n" +
                "当1条物业信息若有多个物业电话是用英文逗号相隔，需每个物业电话满足规则如下：\n" +
                "1、符合电话标准\n" +
                "①11位手机号码(有无\"+86\" 都行)\n" +
                "②7-8位座机号\n" +
                "③3位或4位区号(非必有)加 7-8位座机号 加1-5位分机号(非必有)\n" +
                "-区号与座机号之间有无\"-\"都行\n" +
                "-转机号与座机号之间必须用\"-\"相连\n" +
                "④400/800加七位数字\n" +
                "2、符号可有：英文连接符-，英文逗号, 加号+\n" +
                "3、不允许含有除以上列举之外的特殊符号，不允许中文和中文字符\n" +
                "4、同小区不同物业公司的物业电话不可相同\n" +
                "5、同小区同物业公司电话数量≤5\n" +
                " */\n" +
                "      select '楼盘物业'                    as type,\n" +
                "             'phone'                   as field,\n" +
                "             '物业电话'                    as field_name,\n" +
                "             t1.city_code,\n" +
                "             t1.resblock_id,\n" +
                "             t1.id                     as id,\n" +
                "             t1.phone                  as value,\n" +
                "             concat_ws(\"|\", t1.rule_1) as hit_rule,\n" +
                "             ''                        as field_level,\n" +
                "             ''                        as is_white_list\n" +
                "      from (select resblock_id,\n" +
                "                   id,\n" +
                "                   city_code,\n" +
                "                   phone,\n" +
                "                   phone_s,\n" +
                "                   (case\n" +
                "                        when phone_s regexp '[a-zA-Z\\u4e00-\\u9fa5]' then '规则1:不允许中文和中文字符'\n" +
                "                        when phone_s regexp\n" +
                "                             '' '[`~_!@#$%^&*+=|{}' ' ' ':;' ' ' ',\\\\[\\\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]' ''\n" +
                "                            then '规则2:符号可有：英文连接符-,不允许含有除以上列举之外的特殊符号'\n" +
                "                        when length(phone_s) > 0 and phone_s not regexp\n" +
                "                                                     '^([0-9]{3,4}-?([0-9]{7,8}|[0-9]{3,4})(-[0-9]{3,4})?)$|^1(3[0-9]|4[5-9]|5[0-35-9]|6[2567]|7[0-8]|8[0-9]|9[0-35-9])[0-9]{8}$'\n" +
                "                            then '规则3:不符合电话标准'\n" +
                "                       end) as rule_1\n" +
                "            from property\n" +
                "                     LATERAL VIEW explode(split(phone, \",|/|，|\\\\;|；|、|\\\\s+|\\\\\\\\\")) s_t as phone_s\n" +
                "\n" +
                "            union\n" +
                "\n" +
                "            select p1.*\n" +
                "            from (select resblock_id,\n" +
                "                         id,\n" +
                "                         city_code,\n" +
                "                         phone,\n" +
                "                         phone_s,\n" +
                "                         '规则4:同小区不同物业公司的物业电话不可相同' as rule_1\n" +
                "                  from property\n" +
                "                           LATERAL VIEW explode(split(phone, \",|/|，|\\\\;|；|、|\\\\s+|\\\\\\\\\")) s_t as phone_s\n" +
                "                  where length(phone_s) > 0) p1\n" +
                "\n" +
                "                     inner join\n" +
                "\n" +
                "                 (select resblock_id, phone_s\n" +
                "                  from (\n" +
                "                           select resblock_id\n" +
                "                                , phone_s\n" +
                "                                , count(phone_s)\n" +
                "                           from property\n" +
                "                                    LATERAL VIEW explode(split(phone, \",|/|，|\\\\;|；|、|\\\\s+|\\\\\\\\\")) s_t as phone_s\n" +
                "                           where length(phone_s) > 0\n" +
                "                           group by resblock_id, phone_s\n" +
                "                           having count(phone_s) > 1) t\n" +
                "                 ) p2 on p1.resblock_id = p2.resblock_id and p1.phone_s = p2.phone_s\n" +
                "\n" +
                "            union\n" +
                "--同小区同物业公司电话数量≤5\n" +
                "            select p1.*\n" +
                "            from (select resblock_id,\n" +
                "                         id,\n" +
                "                         city_code,\n" +
                "                         phone,\n" +
                "                         phone_s,\n" +
                "                         '规则5:同小区同物业公司电话数量≤5' as rule_1\n" +
                "                  from property\n" +
                "                           LATERAL VIEW explode(split(phone, \",|/|，|\\\\;|；|、|\\\\s+|\\\\\\\\\")) s_t as phone_s\n" +
                "                  where length(phone_s) > 0) p1\n" +
                "\n" +
                "                     inner join\n" +
                "\n" +
                "                 (select resblock_id, id\n" +
                "                  from (\n" +
                "                           select resblock_id\n" +
                "                                , id\n" +
                "                                , count(phone_s)\n" +
                "                           from property\n" +
                "                                    LATERAL VIEW explode(split(phone, \",|/|，|\\\\;|；|、|\\\\s+|\\\\\\\\\")) s_t as phone_s\n" +
                "                           where length(phone_s) > 0\n" +
                "                           group by resblock_id\n" +
                "                                  , id\n" +
                "                           having count(phone_s) > 5) t\n" +
                "                 ) p2 on p1.resblock_id = p2.resblock_id and p1.id = p2.id\n" +
                "           ) t1\n" +
                "      where rule_1 is not null\n" +
                "     ) temp\n";


        List<String> stringList = splitSql(sql);
        for (String item : stringList) {
            System.out.println(item);
            System.out.println("==============================================");
        }
    }

    /**
     * 提取sql中环境变量Sql
     *
     * @param  sql
     * @return
     */
    public static List<String> extractEnvOfSql(String sql) {
        if (StringUtils.isBlank(sql)) {
            return null;
        }
        List<String> sqlList = splitSqlByNewLine(sql);
        if (CollectionUtils.isEmpty(sqlList)) {
            return sqlList;
        }
        if (CollectionUtils.isEmpty(sqlList)) {
            return sqlList;
        }
        return extractClauseOfSql(sqlList, Lists.newArrayList(SqlClauseEnum.SET_VARIABLE));
    }

    public static List<String> extractClauseOfSql(List<String> sqlList, List<SqlClauseEnum> SqlClauseEnums) {
        if (CollectionUtils.isEmpty(sqlList) || CollectionUtils.isEmpty(SqlClauseEnums)) {
            return null;
        }
        List<String> result = Lists.newArrayListWithCapacity(sqlList.size());
        for (String item : sqlList) {
            List<String> itemChildList = Arrays.stream(item.split("\n"))
                    .filter(e -> StringUtils.isNotBlank(e))
                    .map(e -> e.trim())
                    .filter(e -> SqlClauseEnums.stream().anyMatch(i -> StringUtils.startsWithIgnoreCase(e.toLowerCase(), i.statement)))
                    .collect(Collectors.toList());

            if (CollectionUtils.isNotEmpty(itemChildList)) {
                result.add(StringUtils.join(itemChildList, "\n"));
            }
        }
        return result;
    }

    protected enum SqlClauseEnum{
        SINGLE_LINE_COMMENTS("--", "单行注释")
        ,SET_VARIABLE("set", "set环境变量")
        ;
        /**
         * 语句
         */
        private String statement;
        /**
         * 描述
         */
        private String desc;

        SqlClauseEnum(String statement, String desc){
            this.statement = statement;
            this.desc = desc;
        }
    }

    public static List<String> ignoreStatementOfSql(List<String> sqlList, List<SqlClauseEnum> ignoreStatementOfSqlEnums) {
        // 校验
        if (CollectionUtils.isEmpty(sqlList) || CollectionUtils.isEmpty(ignoreStatementOfSqlEnums)) {
            return null;
        }
        // 按\n换行分隔
        List<String> result = Lists.newArrayListWithCapacity(sqlList.size());
        for (String item : sqlList) {
            List<String> itemChildList = Arrays.stream(item.split("\n"))
                    .filter(e -> StringUtils.isNotBlank(e))
                    .map(e -> e.trim())
                    .filter(e -> !ignoreStatementOfSqlEnums.stream().anyMatch(i -> StringUtils.startsWithIgnoreCase(e.toLowerCase(), i.statement)))
                    .collect(Collectors.toList());

            if (CollectionUtils.isNotEmpty(itemChildList)) {
                result.add(StringUtils.join(itemChildList, "\n"));
            }
        }
        return result;
    }

    public static List<String> splitSqlByNewLine(String sql) {
        String[] sqlArray = sql.split(";\n+?");
        return Arrays.stream(sqlArray)
                .filter(e -> StringUtils.isNotBlank(e))
                .map(e -> e.trim())
                .collect(Collectors.toList());
    }

    public static List<String> splitSql(String sql) {
        if (StringUtils.isBlank(sql)) {
            return null;
        }
        List<String> sqlList = splitSqlByNewLine(sql);
        if (CollectionUtils.isEmpty(sqlList)) {
            return sqlList;
        }

        List<String> result = ignoreStatementOfSql(sqlList, Lists.newArrayList(SqlClauseEnum.SINGLE_LINE_COMMENTS, SqlClauseEnum.SET_VARIABLE));
        return result;
    }

    public static List<HiveSqlParser> getHiveSqlParser(String sql) {
        List<String> sqlList = splitSql(sql);
        List<HiveSqlParser> result = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(sqlList)) {
            for (String item : sqlList) {
                HiveSqlParser hiveSqlParser = new HiveSqlParser();
                try {
                    if(isUserDefineUdfSql(item)) {
                        hiveSqlParser.sqlParser(item);
                        result.add(hiveSqlParser);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new SqlParserException(String.format("获取HiveSqlParser异常，详情：%s", e.toString()));
                }
            }
        }
        return result;
    }

    public static String whetherFixedPartitionsInOutputTable(String sql) {
        List<String> sqlList = splitSql(sql);

        List<HiveSqlParser.OutputTable> outputTables = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(sqlList)) {
            for (String item : sqlList) {
                HiveSqlParser hiveSqlParser = new HiveSqlParser();
                try {
                    if(isUserDefineUdfSql(item)) {
                        hiveSqlParser.sqlParser(item);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("获取输出表中分区出现异常。", e);
                }
                if (CollectionUtils.isNotEmpty(hiveSqlParser.getOutputTableList())) {
                    outputTables.addAll(hiveSqlParser.getOutputTableList());
                }
            }
        }

        if (CollectionUtils.isEmpty(outputTables)) {
            return null;
        }

        List<HiveSqlParser.OutputTable> nonFixedPartitionList = outputTables.stream()
                .filter(e -> e != null)
                .filter(e -> {
                    String value = e.getPartitionValue();
                    if (StringUtils.isBlank(value)) {
                        return true;
                    }
                    value = value.trim();
                    if (StringUtils.startsWithIgnoreCase(value, "'$[")
                            || StringUtils.startsWithIgnoreCase(value, "\"$[")) {
                        return false;
                    } else {
                        return true;
                    }
                })
                .collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(nonFixedPartitionList)) {
            StringBuffer msg = new StringBuffer("解析成功，检测到");
            nonFixedPartitionList.
                    forEach(e -> msg.append("写入表的分区字段：")
                            .append(e.getPartitionColumn())
                            .append("=").append(e.getPartitionValue())
                            .append(","));
            msg.append("不是动态分区，可能会导致每日数据重复，请仔细确认~");
            return msg.toString();
        }
        return null;
    }

    /**
     * 获取SQL中输入输出
     */
    public static Map<String, Set<String>> getIOOfSql(String sql, boolean isExcludeTempTable) {
        Map<String, Set<String>> result = getIOOfSql(sql);
        if (MapUtils.isEmpty(result)) {
            return null;
        }
        if (!isExcludeTempTable) {
            return result;
        }
        Set<String> inputTables = result.get(INPUT);
        Set<String> withTables = result.get(WITH_TABLE);
        if (withTables != null && withTables.size() > 0) {
            if (inputTables != null && inputTables.size() > 0) {
                Set<String> inputTablesOfExcludingTempTable = inputTables.stream().filter(e -> !withTables.contains(e)).collect(Collectors.toSet());
                result.put(INPUT, inputTablesOfExcludingTempTable);
            }
        }
        return result;
    }

    public static Map<String, Set<String>> getIOOfSql(String sql) {
        List<String> sqlList = splitSql(sql);
        if (CollectionUtils.isEmpty(sqlList)) {
            return null;
        }

        Map<String, Set<String>> result = sqlList.stream()
                .filter(StringUtils::isNotBlank)
                .filter(e -> isUserDefineUdfSql(e))
                .map(e -> {
                    LineageInfo lineageUtil = new LineageInfo();
                    try {
                        lineageUtil.getLineageInfo(e);
                        Map<String, Set<String>> ioMap = Maps.newHashMap();
                        Set<String> inputTables = lineageUtil.getInputTableList();
                        if (inputTables != null && inputTables.size() > 0) {
                            ioMap.put(INPUT, inputTables);
                        }
                        Set<String> outputTables = lineageUtil.getOutputTableList();
                        if (outputTables != null && outputTables.size() > 0) {
                            ioMap.put(OUT_PUT, outputTables);
                        }
                        Set<String> withTables = lineageUtil.getWithTableList();
                        if (withTables != null && withTables.size() > 0) {
                            ioMap.put(WITH_TABLE, withTables);
                        }
                        return ioMap;
                    } catch (Exception ex) {
                        throw new SqlParserException(String.format("Sql解析异常，详情：%s", ex.toString()));
                    }
                })
                .filter(Objects::nonNull)
                .flatMap(e -> e.entrySet().stream())
                .flatMap(a -> a.getValue().stream().collect(Collectors.toMap(k -> k, v -> a.getKey(), (k1, k2) -> k2)).entrySet().stream())
                .collect(Collectors.groupingBy(Map.Entry::getValue, Collectors.mapping(item -> item.getKey(), Collectors.toSet())));

        Set<String> outputs = result.getOrDefault(OUT_PUT, Collections.emptySet());
        Set<String> inputs = result.getOrDefault(INPUT, Collections.emptySet());
        Set<String> filteredInputs = inputs.stream().filter(item -> !outputs.contains(item)).collect(Collectors.toSet());
        result.put(INPUT, filteredInputs);
        result.put(OUT_PUT, outputs);

        return result;
    }

    public static boolean isUserDefineUdfSql(String sql) {
        if (sql.toUpperCase().contains("ADD JAR") || sql.toUpperCase().contains("CREATE TEMPORARY FUNCTION")) {
            return false;
        }

        return true;
    }
}
