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

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

import java.util.List;
import java.util.Optional;

public class HiveSqlParser {
    private List<OutputTable> outputTableList = Lists.newArrayList();
    /**
     * 表信息
     */
    private List<Table> tableList = Lists.newArrayList();
    /**
     * 临时表信息
     */
    private List<Table> tempTableList = Lists.newArrayList();


    public static void main(String[] args) {
//        String sql = "CREATE TABLE IF NOT EXISTS test.dwd_find_house_cycle_f_d(a String,b String,c String);\n" +
//                "set hive.tez.container.size=8192;\n" +
//                "set hive.vectorized.execution.enabled=false;\n" +
//                "INSERT OVERWRITE TABLE test.dwd_find_house_cycle_f_d partition(dt='$[yyyyMMdd-1]') select a,b,c from test.bz_rent_contract;"
//                + "INSERT OVERWRITE TABLE test.dwd_find_house_cycle_f_d_1 partition(dt='$[yyyyMMdd-1]') select a,b,c from test.dwd_find_house_cycle_f_d"
//                ;
//        String sql = "set hive.tez.container.size=8192";

//        String sql = "INSERT OVERWRITE TABLE test.dwd_appoint_house_list_0_f_d partition(dt='20210801')\n" +
//                "\n" +
//                "select\n" +
//                "t1.customer_cuid,\n" +
//                "t1.rent_sign_date,\n" +
//                "t1.houseid_roomid,\n" +
//                "t1.rent_contract_code,\n" +
//                "t1.KEEPER_ID,\n" +
//                "t1.city_name,\n" +
//                "t2.user_type,\n" +
//                "t2.houseid_roomid as houseid_roomid2,\n" +
//                "t2.create_time,\n" +
//                "t2.user_id,\n" +
//                "t2.order_num\n" +
//                "from\n" +
//                "(\n" +
//                "  select customer_cuid,\n" +
//                "  to_date(rent_sign_date) rent_sign_date,\n" +
//                "  case when rent_type='分租' then concat('友家',room_id) else concat('整租',house_id) end as houseid_roomid,\n" +
//                "  rent_contract_code,\n" +
//                "  case when rent_contract_code  like \"%-%\" then rent_keeper_code\n" +
//                "           when rent_contract_code not like \"%-%\" and one_level_channel != '2' then rent_keeper_code -- 出房管家\n" +
//                "           when rent_contract_code not like \"%-%\" and one_level_channel = '2' and  two_level_channel='2' then sign_service_keeper_code\n" +
//                "           when rent_contract_code not like \"%-%\" and one_level_channel = '2' and  two_level_channel !='2' then coalesce(accompanying_housekeeper_code,sign_service_keeper_code)\n" +
//                "           when rent_contract_code not like \"%-%\" and rent_channel_name ='直销' then rent_keeper_code\n" +
//                "           else service_keeper_code\n" +
//                "  end as KEEPER_ID,\n" +
//                "  city_name\n" +
//                "  from dwd.dwd_rent_contract_detail_f_d\n" +
//                "  where dt='20210801'\n" +
//                "  and rent_contract_state_code in ('yzf','ysh','dsh','ytz','ybh','ydq')\n" +
//                "  and substr(rent_sign_date,1,10)>='2021-04-01'\n" +
//                "  and rent_channel_name='直销'\n" +
//                ")t1\n" +
//                "left join\n" +
//                "(\n" +
//                "  select\n" +
//                "  '约看' as user_type,\n" +
//                "  case when rent_type='分租' then concat('友家',house_code) else concat('整租',house_id) end as houseid_roomid,\n" +
//                "  to_date(create_time) create_time,\n" +
//                "  user_id,\n" +
//                "  null as order_num\n" +
//                "  from dwd.dwd_cust_reserve_f_d\n" +
//                "  WHERE dt='20210801'\n" +
//                "  and to_date(create_time)>='2021-03-01'\n" +
//                "  and user_id<>'0'\n" +
//                "  and is_del='0'\n" +
//                "\n" +
//                "  union all\n" +
//                "  SELECT\n" +
//                "  '来电' as user_type,\n" +
//                "  case when rent_type='分租' then concat('友家',room_id) else concat('整租',house_id) end as houseid_roomid,\n" +
//                "  to_date(answer_time_begin) create_time,\n" +
//                "  user_id,\n" +
//                "  null as order_num\n" +
//                "  from dwd.dwd_cust_call_f_d\n" +
//                "  where dt='20210801'\n" +
//                "  and to_date(answer_time_begin)>='2021-03-01'\n" +
//                "\n" +
//                "  union all\n" +
//                "  SELECT\n" +
//                "  'IM' as user_type,\n" +
//                "  case when rent_type='分租' then concat('友家',room_id) else concat('整租',house_id) end as houseid_roomid,\n" +
//                "  to_date(from_unixtime(cast(createtime/1000 as bigint),'yyyy-MM-dd')) create_time,\n" +
//                "  clientuid,\n" +
//                "  null as order_num\n" +
//                "  from dwd.dwd_cust_im_keeper_i_d\n" +
//                "  where dt>='20210301'\n" +
//                "  and fromRoleType='ROLE_CLIENT'\n" +
//                "  and invNo is not null\n" +
//                "\n" +
//                "  union all\n" +
//                "  SELECT\n" +
//                "  case when is_del=0 and signin=1 and dtl_is_del=0 then '真实带看' else '其他带看' end as user_type,\n" +
//                "  case when house_code is not null and house_code != '' then concat('友家',house_code) else concat('整租',house_id) end houseid_roomid,\n" +
//                "  to_date(create_time) ld_date,\n" +
//                "  user_id,\n" +
//                "  order_num\n" +
//                "  from dwd.dwd_cust_appoint_f_d\n" +
//                "  where dt = '20210801'\n" +
//                "  and create_time>='2021-03-01 00:00:00'\n" +
//                ")t2 on t1.customer_cuid = t2.user_id and t2.create_time <= t1.rent_sign_date and t2.create_time >= date_sub(t1.rent_sign_date,30);";

        String sql = "INSERT OVERWRITE TABLE test.bz_user partition(dt='20210801') select * from test.dep where dt='20210801' and dt = '$[yyyyMMdd-1]' and dt = '$[20210801]'";

//        String sql = "INSERT OVERWRITE TABLE bl_bi.competitor_lvzy777 partition(dt='$[yyyyMMdd]')\n" +
//        String sql = "INSERT OVERWRITE TABLE bl_bi.competitor_lvzy777 partition(dt='20211012')\n" +
//                "select \n" +
//                "substr(a1.city,1,2) city,\n" +
//                "a1.loupan_id,\n" +
//                "a1.loupan,\n" +
//                "a1.huxing,\n" +
//                "a1.guapaicnt,\n" +
//                "a1.avgarea_d,\n" +
//                "a1.avgprice_d,\n" +
//                "b1.avgarea_t,\n" +
//                "b1.avgprice_t,\n" +
//                "b1.chufangcnt,\n" +
//                "b1.housecnt\n" +
//                "from(\n" +
//                "  select \n" +
//                "  a.city,\n" +
//                "  a.loupan_id,\n" +
//                "  a.loupan,\n" +
//                "  COALESCE(case when a.chuzufangshi='合租' or substr(a.huxing,1,1)>=3 then '合租'\n" +
//                "    when ( a.chuzufangshi='整租') \n" +
//                "     and substr(a.huxing,1,2)='1室'  then '整租一居'\n" +
//                "    when (  a.chuzufangshi='整租') \n" +
//                "     and substr(a.huxing,1,2)='2室'  then '整租二居' end,\n" +
//                "    case when b.type='合租' or b.frame_bedroom_num>=3  then '合租'\n" +
//                "    when b.type='整租' and b.frame_bedroom_num=1 then '整租一居'\n" +
//                "    when b.type='整租' and b.frame_bedroom_num=2 then '整租二居'\n" +
//                "    end) huxing,\n" +
//                "  avg(regexp_replace(a.mianji,'[^0-9]','')) avgarea_d,\n" +
//                "  avg(price) avgprice_d,\n" +
//                "  count(distinct a.fangyuanhao) guapaicnt \n" +
//                "from dwd.dwd_displayer_com_pro_data_f_d a \n" +
//                "left join(\n" +
//                "  select housecode,\n" +
//                "  frame_bedroom_num,\n" +
//                "  type\n" +
//                "  from dwd.dwd_displayer_lj_app_info_f_d\n" +
//                "  where dt='$[yyyyMMdd]'\n" +
//                ")  b on a.fangyuanhao=b.housecode\n" +
//                "where dt='$[yyyyMMdd]'\n" +
//                "and channel='beike'\n" +
//                "and a.company like '%链家%'\n" +
//                "group by \n" +
//                "a.city,\n" +
//                "a.loupan_id,\n" +
//                "  a.loupan,\n" +
//                " COALESCE(case when a.chuzufangshi='合租' or substr(a.huxing,1,1)>=3 then '合租'\n" +
//                "    when ( a.chuzufangshi='整租') \n" +
//                "     and substr(a.huxing,1,2)='1室'  then '整租一居'\n" +
//                "    when (  a.chuzufangshi='整租') \n" +
//                "     and substr(a.huxing,1,2)='2室'  then '整租二居' end,\n" +
//                "    case when b.type='合租' or b.frame_bedroom_num>=3  then '合租'\n" +
//                "    when b.type='整租' and b.frame_bedroom_num=1 then '整租一居'\n" +
//                "    when b.type='整租' and b.frame_bedroom_num=2 then '整租二居'\n" +
//                "    end)\n" +
//                ")  a1 \n" +
//                "left join(\n" +
//                " select w1.loupan,\n" +
//                " COALESCE(case when w2.chuzufangshi='合租' or substr(w2.huxing,1,1)>=3 then '合租'\n" +
//                " when ( w2.chuzufangshi='整租') \n" +
//                " and substr(w2.huxing,1,2)='1室'  then '整租一居'\n" +
//                " when ( w2.chuzufangshi='整租') \n" +
//                " and substr(w2.huxing,1,2)='2室'  then '整租二居' end,\n" +
//                " case when w1.type='合租' or w1.frame_bedroom_num>=3 then '合租'\n" +
//                " when w1.type='整租' and w1.frame_bedroom_num=1 then '整租一居'\n" +
//                " when w1.type='整租' and w1.frame_bedroom_num=2 then '整租二居'\n" +
//                "end) huxing,\n" +
//                "avg(regexp_replace(w1.mianji,'[^0-9]','')) avgarea_t,\n" +
//                "avg(w1.price) avgprice_t,\n" +
//                "count(distinct concat(w1.housecode,w1.transaction_time)) chufangcnt,\n" +
//                "count(distinct w1.housecode) housecnt\n" +
//                "from(\n" +
//                "select loupan,\n" +
//                "type,frame_bedroom_num,mianji,price,housecode,transaction_time\n" +
//                "from dwd.dwd_displayer_lj_app_info_f_d\n" +
//                "where  dt='$[yyyyMMdd]'\n" +
//                "and transaction_time>=\"20-11-01\"\n" +
//                ")  w1\n" +
//                "left join (\n" +
//                "select fangyuanhao,huxing,mianji,\n" +
//                "chuzufangshi\n" +
//                "from dwd.dwd_displayer_com_pro_data_f_d\n" +
//                "where dt='$[yyyyMMdd]'\n" +
//                "and channel='beike'\n" +
//                "and company like '%链家%'\n" +
//                ") w2 on w1.housecode=w2.fangyuanhao\n" +
//                "\n" +
//                "group by \n" +
//                "w1.loupan,\n" +
//                " COALESCE(case when w2.chuzufangshi='合租' or substr(w2.huxing,1,1)>=3 then '合租'\n" +
//                " when ( w2.chuzufangshi='整租') \n" +
//                " and substr(w2.huxing,1,2)='1室'  then '整租一居'\n" +
//                " when ( w2.chuzufangshi='整租') \n" +
//                " and substr(w2.huxing,1,2)='2室'  then '整租二居' end,\n" +
//                " case when w1.type='合租' or w1.frame_bedroom_num>=3 then '合租'\n" +
//                " when w1.type='整租' and w1.frame_bedroom_num=1 then '整租一居'\n" +
//                " when w1.type='整租' and w1.frame_bedroom_num=2 then '整租二居'\n" +
//                "end)\n" +
//                ") b1 on a1.loupan=b1.loupan and a1.huxing=b1.huxing";

//       String sql = "update testdb.test set a = 1 where name = 'zhangsan'";
//        String sql = "delete from testdb.test where name = 'zhangsan'";
//        String sql = "select * from testdb.test ";
//        String sql = "CREATE TABLE IF NOT EXISTS test.dwd_find_house_cycle_f_d(a String,b String,c String)";
//        String sql = "-- 注释\n" +
//                "create table if not exists dws.dws_hire_hold_vacancy_cnt_f_m(\n" +
//                "execution_time                                                string comment '执行时间(相当于分区时间只是格式不同)',\n" +
//                "country_name                                                  string comment'全国',\n" +
//                "city_code                                                     string comment'城市code',\n" +
//                "city_name                                                     string comment'城市name',\n" +
//                "deptid_l2                                                     string comment'二级部门',\n" +
//                "deptid_l2_name                                                string comment'二级部门描述',\n" +
//                "deptid_l2_plus                                                string comment'二级部门PLUS',\n" +
//                "deptid_l2_name_plus                                           string comment'二级部门PLUS描述',\n" +
//                "deptid_l3                                                     string comment'三级部门代码,战区code',\n" +
//                "deptid_l3_name                                                string comment'三级部门,战区name',\n" +
//                "deptid_l4                                                     string comment'四级部门代码,大区code',\n" +
//                "deptid_l4_name                                                string comment'四级部门,大区name',\n" +
//                "deptid_l5                                                     string comment'五级部门代码,业务组code',\n" +
//                "deptid_l5_name                                                string comment'五级部门,业务组name',\n" +
//                "keeper_code                                                   string comment'所属服务管家code',\n" +
//                "keeper_name                                                   string comment'所属服务管家name',\n" +
//                "resblock_id                                                   string comment'楼盘code',\n" +
//                "resblock_name                                                 string comment'楼盘name',\n" +
//                "product_version_code                                          string comment'产品版本',\n" +
//                "product_version_name                                          string comment'产品版本名称',\n" +
//                "rent_type                                                     string comment'可出租方式',\n" +
//                "after_reform_bedroom_amount                                   string comment'改后户型',\n" +
//                "product_type                                                  string comment'产品类型',\n" +
//                "vacancy_days                                                  string comment'空置天数',\n" +
//                "vacancy_type                                                  string comment'空置类型',\n" +
//                "face                                                          string comment'朝向',\n" +
//                "size                                                          string comment'面积',\n" +
//                "is_balcony                                                    string comment'是否有阳台 1是 0否',\n" +
//                "is_inde_toilet                                                string comment'是否有独卫 1是 0否',\n" +
//                "room_type_code                                                string comment'房间类型code',\n" +
//                "room_type_name                                                string comment'房间类型name',\n" +
//                "`floor`                                                        string comment'楼层',\n" +
//                "floor_total                                                   string comment'总楼层',\n" +
//                "bad_floor                                                     string comment'一/顶层',\n" +
//                "hold_rooms_house_cnt                                          string comment'总持有间套',\n" +
//                "vacancy_rooms_house_cnt                                       string comment'总空置间套',\n" +
//                "all_vacancy_days                                              string comment'总空置天数',\n" +
//                "rent_rooms_house_cnt                                          string comment'在租间套',\n" +
//                "off_shelf_stock_cnt                                           string comment'下架库存量',\n" +
//                "stock_vacancy_rent_started_wait_first_config_cnt              string comment'空置(已起租)待首次配置',\n" +
//                "stock_vacancy_rent_started_new_hire_config_cnt                string comment'空置(已起租)新收配置中',\n" +
//                "stock_vacancy_rent_started_wait_rent_air_check_cnt            string comment'空置(已起租)待租空检中',\n" +
//                "stock_vacancy_rent_started_sec_config_cnt                     string comment'空置(已起租)二次配置中',\n" +
//                "stock_vacancy_rent_started_wait_rent_remove_air_check_cnt     string comment'空置(已起租)待租中(不含空检)',\n" +
//                "stock_vacancy_rent_started_cancel_vacancy_cnt                 string comment'空置(已起租)解约空置中',\n" +
//                "stock_vacancy_rent_started_cancel_rent_cnt                    string comment'空置(已起租)解约在租中',\n" +
//                "stock_vacancy_rent_started_cancel_cnt                         string comment'空置(已起租)业主解约中',\n" +
//                "stock_vacancy_rooms_house_rent_started_cnt                    string comment'空置(已起租)',\n" +
//                "stock_vacancy_rooms_house_no_rent_cnt                         string comment'空置(未起租)',\n" +
//                "all_vacancy_days_over_100                                     string comment'百日库存',\n" +
//                "keeper_type_code                                              string COMMENT '管家类型code',\n" +
//                "keeper_type_name                                              string COMMENT '管家类型name',\n" +
//                "trusteeship_type_code                                         string COMMENT '托管模式code',\n" +
//                "trusteeship_type_name                                         string COMMENT '托管模式name',\n" +
//                "house_no                                                      string COMMENT '房屋编号',\n" +
//                "stock_cancel_before_rent_cnt                                  string COMMENT '首次出租前解约'\n" +
//                ")\n" +
//                "comment '每10min更新一次,库存明细表:可以统计持有量、空置量、入住率、平均空置天数等指标，每个分区存的是全量数据，粒度到间套(整租一条记录，分租到间)；包含房屋的状态流转信息；产品，管家，城市，楼盘信息；促销价格(app上显示的价格)和销售价格(收房时定的出房价)；出房和收房的合同号以及日期、状态信息；包含房屋房间的属性信息(朝向、面积、户型、房间类型等)，涵盖房屋表和房间表信息，可以作为间套粒度的房屋房间表使用，如果想获取到房屋粒度的属性可以使用房屋表，如果想获取到房间粒度的属性可以使用房间表'\n" +
//                "partitioned by (dt_hour_minute string comment 'format is %Y%m%d%H%M')\n" +
//                "row format delimited\n" +
//                " fields terminated by '\\001'\n" +
//                " lines terminated by '\\n'\n" +
//                "stored as orc\n" +
//                "tblproperties(\"data_owner\"=\"wangmw\", \"business_owner\"=\"wangmw\", \"transactional\"=\"false\",\"partition.retention.period\"=\"15d\",\"discover.partitions\"=\"true\",\"orc.compress\"=\"snappy\",\"tags\"=\"库存,持有,空置,空置天数,入住率,平均空置天数 每10min更新一次\")";
//        String sql = "with\n" +
//                "tmp_config_contract as (select a.hire_contract_id\n" +
//                "                               ,hire_contract_code\n" +
//                "                               ,is_goods_share\n" +
//                "                               ,nvl(if(total_config_list_price=0,total_config_cost,total_config_list_price),0) as total_config_list_price\n" +
//                "                               ,nvl(if(broadband_list_price is null,broadband_cost,broadband_list_price),0) as broadband_list_price\n" +
//                "                               ,configuration_type\n" +
//                "                          from (select hire_contract_id\n" +
//                "                                       ,is_goods_share\n" +
//                "                                       ,total_config_list_price\n" +
//                "                                       ,broadband_list_price\n" +
//                "                                       ,configuration_type\n" +
//                "                                       ,total_config_cost\n" +
//                "                                       ,broadband_cost\n" +
//                "                                   from dws.dws_config_each_item_cost_f_d\n" +
//                "                                  where dt='$[yyyyMMdd-1]')a\n" +
//                "                          left join (select hire_contract_id\n" +
//                "                                            ,hire_contract_code\n" +
//                "                                       from dwd.dwd_hire_contract_detail_f_d\n" +
//                "                                      where dt='$[yyyyMMdd-1]') b\n" +
//                "                            on a.hire_contract_id=b.hire_contract_id\n" +
//                "                         ),\n" +
//                "--获取新收或者续约配置成本\n" +
//                "tmp_total_fee as(select ta.hire_contract_code\n" +
//                "                        ,tb.hire_contract_id\n" +
//                "                        ,share_fee / (case when tb.hire_year > 5\n" +
//                "                 \t                     then 60\n" +
//                "                                          else tb.hire_year * 12\n" +
//                "                                           end)\n" +
//                "                 \t    +\n" +
//                "                 \t    unshare_fee / 12 as new_hire_or_renew_month_share_fee\n" +
//                "                        ,ta.total_fee    as new_hire_or_renew_total_fee\n" +
//                "                        ,ta.share_fee    as new_hire_or_renew_total_sharefee\n" +
//                "                        ,ta.unshare_fee  as new_hire_or_renew_total_unsharefee\n" +
//                "                        ,ta.net_fee      as new_hire_or_renew_net_fee\n" +
//                "                   from (select cb.hire_contract_code,\n" +
//                "                                sum(case when cb.is_goods_share = 0\n" +
//                "                 \t\t\t            then total_config_list_price\n" +
//                "                                         else 0\n" +
//                "                                          end) unshare_fee,\n" +
//                "                                sum(case when cb.is_goods_share = 1 then total_config_list_price\n" +
//                "                                         else 0\n" +
//                "                                          end) share_fee,\n" +
//                "                                sum(total_config_list_price) total_fee,\n" +
//                "                                sum(nvl(cb.broadband_list_price, 0)) net_fee\n" +
//                "                           from tmp_config_contract cb\n" +
//                "                          where cb.configuration_type in (0, 1)\n" +
//                "                            and cb.hire_contract_code not like '%-%'\n" +
//                "                          group by cb.hire_contract_code\n" +
//                "\n" +
//                "                          union all\n" +
//                "\n" +
//                "                         select cb.hire_contract_code,\n" +
//                "                                sum(case when cb.is_goods_share = 0\n" +
//                "                 \t\t\t\t        then total_config_list_price\n" +
//                "                                         else 0\n" +
//                "                                          end) unshare_fee,\n" +
//                "                                sum(case when cb.is_goods_share = 1\n" +
//                "                 \t\t\t\t        then total_config_list_price\n" +
//                "                                         else 0\n" +
//                "                                          end) share_fee,\n" +
//                "                                sum(total_config_list_price) total_fee,\n" +
//                "                                sum(nvl(cb.broadband_list_price, 0)) net_fee\n" +
//                "                           from tmp_config_contract cb\n" +
//                "                          where cb.configuration_type in (7, 8)\n" +
//                "                            and cb.hire_contract_code like '%-%'\n" +
//                "                          group by cb.hire_contract_code\n" +
//                "                         ) ta\n" +
//                "\t\t\t\t   join (select hire_contract_id\n" +
//                "                                ,hire_contract_code\n" +
//                "\t\t\t\t\t\t        ,hire_year\n" +
//                "                           from dwd.dwd_hire_contract_detail_f_d\n" +
//                "                          where dt='$[yyyyMMdd-1]') tb\n" +
//                "\t\t\t\t\t on ta.hire_contract_code=tb.hire_contract_code)\n" +
//                "\n" +
//                "--在收房合同明细的基础上增加配置成本\n" +
//                "insert overwrite table dws.dws_hire_contract_f_d partition(dt='$[yyyyMMdd-1]')\n" +
//                "select contract.hire_contract_id                   --合同id\n" +
//                "       ,contract.hire_contract_code                 --收房合同号\n" +
//                "       ,contract.house_id                           --房屋id\n" +
//                "       ,contract.house_code                         --房屋编码\n" +
//                "       ,contract.house_source_code                  --房源编码\n" +
//                "       ,contract.product_version_code               --产品版本code\n" +
//                "\t   ,contract.product_version_name               --产品版本名称\n" +
//                "\t   ,contract.product_line_code                  --产品线code\n" +
//                "\t   ,contract.product_line_name                  --产品线名称\n" +
//                "\t   ,contract.rent_type                          --可出租方式\n" +
//                "       ,contract.payment_cycle_code                 --付款方式,@1:月付,@3:季付,@6:半年付,@12:年付\n" +
//                "\t   ,contract.hire_contract_state_code           --合同签约状态@wqy:未签约,@yqy:已签约,@lyz:”履约中,@ydq:已到期,@yzf:已作废,@ytz:已退租, @jyz:解约中,@yjy:已解约\n" +
//                "       ,contract.hire_contract_state_name           --合同签约状态@wqy:未签约,@yqy:已签约,@lyz:”履约中,@ydq:已到期,@yzf:已作废,@ytz:已退租, @jyz:解约中,@yjy:已解约\n" +
//                "       ,contract.city_code                          --城市code\n" +
//                "\t   ,contract.city_name                          --城市name\n" +
//                "       ,contract.resblock_id                        --楼盘code\n" +
//                "       ,contract.resblock_name                      --楼盘name\n" +
//                "\t   ,contract.before_reform_bedroom_amount       --优化前卧室数量\n" +
//                "       ,contract.before_reform_livingroom_amount    --优化前客厅数量\n" +
//                "       ,contract.after_reform_bedroom_amount        --优化后卧室数量\n" +
//                "       ,contract.after_reform_livingroom_amount     --优化后客厅数量\n" +
//                "       ,contract.after_reform_kitchen_amount        --优化后厨房数量\n" +
//                "       ,contract.after_reform_bathroom_amount       --优化后卫生间数量\n" +
//                "       ,contract.decorate_degree_code               --装修程度code\n" +
//                "       ,contract.decorate_degree_name               --装修程度name\n" +
//                "       ,contract.hire_sign_date                     --合同签约日期\n" +
//                "\t   ,contract.hire_confirm_date                  --业主确认日期（已签约日期）\n" +
//                "\t   ,contract.hire_complete_sign_date            --兼容签约日期和业主确认日期,统计收房量使用\n" +
//                "\t   ,contract.hire_contract_type_code            --合同类型code：1续约，0新签\n" +
//                "\t   ,contract.hire_contract_type_name            --合同类型name：1续约，0新签\n" +
//                "\t   ,contract.hire_channel_code                  --收房渠道代码：1续约，0渠道，2综合管家直收，3直收管家直收\n" +
//                "\t   ,contract.hire_channel_name                  --收房渠道名称：1续约，0渠道，2综合管家直收，3直收管家直收\n" +
//                "       ,contract.hire_year                          --签约年\n" +
//                "       ,contract.hire_month                         --签约月\n" +
//                "       ,contract.hire_month_price                   --收房签约月租金\n" +
//                "       ,contract.current_hire_month_price           --实际收房签约月租金(有涨幅)\n" +
//                "       ,contract.hire_start_date                   --合同起租日\n" +
//                "       ,contract.hire_end_date                     --合同截止日\n" +
//                "\t   ,contract.accept_time                        --竣工验收日期\n" +
//                "\t   ,contract.amount_house_date                  --量房日期\n" +
//                "\t   ,config.new_hire_or_renew_total_fee\t       --总配置费用\n" +
//                "       ,config.new_hire_or_renew_total_sharefee   --分摊费用总和\n" +
//                "       ,config.new_hire_or_renew_total_unsharefee --不分摊费用综合\n" +
//                "       ,config.new_hire_or_renew_month_share_fee  --月分摊成本\n" +
//                "       ,config.new_hire_or_renew_net_fee          --宽带费用\n" +
//                "\t   ,contract.old_hire_contract_id               --上份合同ID\n" +
//                "       ,contract.owner_id                           --业主id\n" +
//                "       ,contract.house_service_keeper_code          --服务管家系统号\n" +
//                "       ,contract.house_service_keeper_name          --服务管家姓名\n" +
//                "       ,contract.house_belong_keeper_code           --房屋所属管家系统号\n" +
//                "       ,contract.house_belong_keeper_name           --房屋所属管家姓名\n" +
//                "       ,contract.hire_keeper_code                   --自如管家系统号\n" +
//                "       ,contract.hire_keeper_name                   --自如管家姓名\n" +
//                "       ,contract.is_direct_har                      --业主来源\n" +
//                "       ,contract.audit_state                        --合同签约状态,@-1:未提交审核,@0:待审核,@1:审核未通过,@2:审核通过\n" +
//                "       ,contract.audit_date                         --审核通过时间\n" +
//                "       ,contract.valuation_model_id                 --计价模型id\n" +
//                "       ,contract.property_type                      --产权类型\n" +
//                "       ,contract.biz_opp_id                         --商机id\n" +
//                "       ,contract.hire_conf_staff_code               --收房配置专员系统号\n" +
//                "       ,contract.hire_conf_staff_name               --收房配置专员姓名\n" +
//                "       ,contract.hire_belong_conf_staff_code        --所属配置专员系统号\n" +
//                "       ,contract.hire_belong_conf_staff_name        --所属配置专员姓名\n" +
//                "       ,contract.is_del                             --是否删除,1是,0否\n" +
//                "\t   ,contract.is_only_save                       --是否是草稿,1是,0否\n" +
//                "       ,contract.create_date                        --创建日期\n" +
//                "       ,contract.last_modify_time                   --更新日期\n" +
//                "       ,contract.data_source_system                 --数据系统来源'\n" +
//                "       ,contract.hand_over_date\n" +
//                "       ,contract.trusteeship_type_code              --托管模式code\n" +
//                "       ,contract.trusteeship_type_name              --托管模式name\n" +
//                "  from (select hire_contract_id                   --合同id\n" +
//                "               ,hire_contract_code                 --收房合同号\n" +
//                "               ,house_id                           --房屋id\n" +
//                "               ,house_code                         --房屋编码\n" +
//                "               ,house_source_code                  --房源编码\n" +
//                "               ,product_version_code               --产品版本code\n" +
//                "\t           ,product_version_name               --产品版本名称\n" +
//                "\t           ,product_line_code                  --产品线code\n" +
//                "\t           ,product_line_name                  --产品线名称\n" +
//                "\t           ,rent_type                          --可出租方式\n" +
//                "               ,payment_cycle_code                 --付款方式,@1:月付,@3:季付,@6:半年付,@12:年付\n" +
//                "\t           ,hire_contract_state_code           --合同签约状态@wqy:未签约,@yqy:已签约,@lyz:”履约中,@ydq:已到期,@yzf:已作废,@ytz:已退租, @jyz:解约中,@yjy:已解约\n" +
//                "               ,hire_contract_state_name           --合同签约状态@wqy:未签约,@yqy:已签约,@lyz:”履约中,@ydq:已到期,@yzf:已作废,@ytz:已退租, @jyz:解约中,@yjy:已解约\n" +
//                "               ,city_code                          --城市code\n" +
//                "\t           ,city_name                          --城市name\n" +
//                "               ,resblock_id                        --楼盘code\n" +
//                "               ,resblock_name                      --楼盘name\n" +
//                "\t           ,before_reform_bedroom_amount       --优化前卧室数量\n" +
//                "               ,before_reform_livingroom_amount    --优化前客厅数量\n" +
//                "               ,after_reform_bedroom_amount        --优化后卧室数量\n" +
//                "               ,after_reform_livingroom_amount     --优化后客厅数量\n" +
//                "               ,after_reform_kitchen_amount        --优化后厨房数量\n" +
//                "               ,after_reform_bathroom_amount       --优化后卫生间数量\n" +
//                "               ,decorate_degree_code               --装修程度code\n" +
//                "               ,decorate_degree_name               --装修程度name\n" +
//                "               ,hire_sign_date                     --合同签约日期\n" +
//                "\t           ,hire_confirm_date                  --业主确认日期（已签约日期）\n" +
//                "\t           ,hire_complete_sign_date            --兼容签约日期和业主确认日期,统计收房量使用\n" +
//                "\t           ,hire_contract_type_code            --合同类型code：1续约，0新签\n" +
//                "\t           ,hire_contract_type_name            --合同类型name：1续约，0新签\n" +
//                "\t           ,hire_channel_code                  --收房渠道代码：1续约，0渠道，2综合管家直收，3直收管家直收\n" +
//                "\t           ,hire_channel_name                  --收房渠道名称：1续约，0渠道，2综合管家直收，3直收管家直收\n" +
//                "               ,hire_year                          --签约年\n" +
//                "               ,hire_month                         --签约月\n" +
//                "               ,hire_month_price                   --收房签约月租金\n" +
//                "               ,current_hire_month_price           --实际收房签约月租金(有涨幅)\n" +
//                "               ,hire_start_date                   --合同起租日\n" +
//                "               ,hire_end_date                     --合同截止日\n" +
//                "\t           ,accept_time                        --竣工验收日期\n" +
//                "\t           ,amount_house_date                  --量房日期\n" +
//                "\t           ,old_hire_contract_id               --上份合同ID\n" +
//                "               ,owner_id                           --业主id\n" +
//                "               ,house_service_keeper_code          --服务管家系统号\n" +
//                "               ,house_service_keeper_name          --服务管家姓名\n" +
//                "               ,house_belong_keeper_code           --房屋所属管家系统号\n" +
//                "               ,house_belong_keeper_name           --房屋所属管家姓名\n" +
//                "               ,hire_keeper_code                   --自如管家系统号\n" +
//                "               ,hire_keeper_name                   --自如管家姓名\n" +
//                "               ,is_direct_har                      --业主来源\n" +
//                "               ,audit_state                        --合同签约状态,@-1:未提交审核,@0:待审核,@1:审核未通过,@2:审核通过\n" +
//                "               ,audit_date                         --审核通过时间\n" +
//                "               ,valuation_model_id                 --计价模型id\n" +
//                "               ,property_type                      --产权类型\n" +
//                "               ,biz_opp_id                         --商机id\n" +
//                "               ,hire_conf_staff_code               --收房配置专员系统号\n" +
//                "               ,hire_conf_staff_name               --收房配置专员姓名\n" +
//                "               ,hire_belong_conf_staff_code        --所属配置专员系统号\n" +
//                "               ,hire_belong_conf_staff_name        --所属配置专员姓名\n" +
//                "               ,is_del                             --是否删除,1是,0否\n" +
//                "\t           ,is_only_save                       --是否是草稿,1是,0否\n" +
//                "               ,create_date                        --创建日期\n" +
//                "               ,last_modify_time                   --更新日期\n" +
//                "               ,data_source_system                 --数据系统来源'\n" +
//                "               ,hand_over_date\n" +
//                "               ,trusteeship_type_code              --托管模式code\n" +
//                "               ,trusteeship_type_name              --托管模式name\n" +
//                "          from dwd.dwd_hire_contract_detail_f_d\n" +
//                "\t\t where dt='$[yyyyMMdd-1]') contract\n" +
//                "  left join tmp_total_fee config\n" +
//                "    on contract.hire_contract_id = config.hire_contract_id\n";
//        String sql = "insert overwrite table db_test.test partition(dt='$[yyyyMMdd-1]') select * from db_test.dep ";
//        String sql = "insert into table db_test.test partition(dt='$[yyyyMMdd-1]') select * from db_test.dep  partition(dt='$[yyyyMMdd-1]')";
//        String sql = "drop table db_test.test";
//        String sql = "CREATE EXTERNAL TABLE `crm_rent_t_rent_lease_sync_log_f_d`(\n" +
//                "`id` string COMMENT 'from deserializer', \n" +
//                "`contract_code` string COMMENT 'from deserializer', \n" +
//                "`lease_id` string COMMENT 'from deserializer', \n" +
//                "`in_params` string COMMENT 'from deserializer', \n" +
//                "`out_params` string COMMENT 'from deserializer', \n" +
//                "`state` string COMMENT 'from deserializer', \n" +
//                "`c_time` string COMMENT 'from deserializer')\n" +
//                "ROW FORMAT SERDE \n" +
//                "'org.apache.hive.storage.jdbc.JdbcSerDe' \n" +
//                "STORED BY \n" +
//                "'org.apache.hive.storage.jdbc.JdbcStorageHandler' \n" +
//                "WITH SERDEPROPERTIES ( \n" +
//                "'serialization.format'='1')\n" +
//                "TBLPROPERTIES (\n" +
//                "'bucketing_version'='2', \n" +
//                "'discover.partitions'='true', \n" +
//                "'hive.sql.database.type'='MYSQL', \n" +
//                "'hive.sql.dbcp.password.key'='dev_data_center.password', \n" +
//                "'hive.sql.dbcp.password.keystore'='jceks://hdfs/user/hive/password.jceks', \n" +
//                "'hive.sql.dbcp.username'='dev_data_center', \n" +
//                "'hive.sql.jdbc.driver'='com.mysql.jdbc.Driver', \n" +
//                "'hive.sql.jdbc.fetch.size'='2000', \n" +
//                "'hive.sql.jdbc.url'='jdbc:mysql://crm.crmrent.mdbm.datawarehouse.com:3306?tinyInt1isBit=false&serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=convertToNull&useCursorFetch=true&characterEncoding=utf-8&useSSL=false', \n" +
//                "'hive.sql.query'='select `id` AS `id`,`contract_code` AS `contract_code`,`lease_id` AS `lease_id`,`in_params` AS `in_params`,`out_params` AS `out_params`,`state` AS `state`,`c_time` AS `c_time` from crm_rent.t_rent_lease_sync_log', \n" +
//                "'transient_lastDdlTime'='1598598546')";
//        String sql = "--带有注释的sql测试\n" +
//                "--带有注释的sql测试\n" +
//                "--带有注释的sql测试\n" +
//                "--带有注释的sql测试\n" +
//                "insert overwrite table test.dws_hire_hold_vacancy_cnt_f_m_gaigai03 partition(dt_hour_minute=\"$[yyyyMMdd]$[HHmmss+0/24/60]\")\n" +
//                "select \"$[yyyyMMdd]$[HHmmss+0/24/60]\" execution_time\n" +
//                "       ,hr.country_name\n" +
//                "       ,hr.city_code\n" +
//                "       ,hr.city_name\n" +
//                "       ,hr.deptid_l2\n" +
//                "       ,hr.deptid_l2_name\n" +
//                "       ,hr.deptid_l2_plus\n" +
//                "       ,hr.deptid_l2_name_plus\n" +
//                "       ,hr.deptid_l3\n" +
//                "       ,hr.deptid_l3_name\n" +
//                "       ,hr.deptid_l4\n" +
//                "       ,hr.deptid_l4_name\n" +
//                "       ,hr.deptid_l5\n" +
//                "       ,hr.deptid_l5_name\n" +
//                "       ,core.manager_code AS keeper_code\n" +
//                "       ,hr.staff_name AS keeper_name\n" +
//                "       ,core.resblock_id\n" +
//                "       ,core.resblock_name\n" +
//                "       ,core.ziroom_version AS product_version_code\n" +
//                "       ,core.ziroom_version_name AS product_version_name\n" +
//                "       ,case when core.is_whole ='1' then '整租' else '友家' end rent_type\n" +
//                "       ,core.bedroom_count AS after_reform_bedroom_amount\n" +
//                "       ,case when core.is_whole ='0' then '友家'\n" +
//                "             when core.is_whole ='1' and core.bedroom_count=1 then '整租一居'\n" +
//                "             when core.is_whole ='1' and core.bedroom_count>1 then '整租二居及以上'\n" +
//                "                 end product_type\n" +
//                "       ,core.vacancy_days as vacancy_days\n" +
//                "       ,core.vacancy_type\n" +
//                "       ,core.face\n" +
//                "       ,core.size\n" +
//                "       ,core.has_balcony As is_balcony\n" +
//                "       ,core.has_toilet As is_inde_toilet\n" +
//                "       ,core.room_style_code As room_type_code\n" +
//                "       ,case when core.room_style_code = '38000101' then '主卧'\n" +
//                "             when core.room_style_code = '38000102' then '次卧'\n" +
//                "             when core.room_style_code = '38000103' then '优化间'\n" +
//                "                 end room_type_name\n" +
//                "       ,core.`floor`\n" +
//                "       ,core.floor_total\n" +
//                "       ,case when core.`floor`=1 then '一层'\n" +
//                "             when core.`floor`=floor_total then '顶层'\n" +
//                "                 end bad_floor\n" +
//                "       ,count(distinct case when core.inv_status<>'402' then concat(core.house_id,case when core.room_id is null then '' else core.room_id end ) else null end)\n" +
//                "                 as hold_rooms_house_cnt                                      --总持有间套\n" +
//                "       ,count(distinct case when core.rent_status = '0' and core.inv_status<>'402' then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as vacancy_rooms_house_cnt                                   --总空置间套\n" +
//                "       ,sum(case when core.rent_status = '0' and core.inv_status<>'402' then core.vacancy_days else 0 end)\n" +
//                "                 as all_vacancy_days                                              --总空置天数\n" +
//                "       ,count(distinct case when core.rent_status = '1' and core.inv_status<>'402' then concat(core.house_id,case when core.room_id is null then '' else core.room_id end ) else null end)\n" +
//                "                 as rent_rooms_house_cnt                                      --在租间套\n" +
//                "       ,count(distinct case when (core.abnormal_code<>'' OR core.abnormal_code IS NOT NULL) and regexp_replace(core.abnormal_code,'\\\"101\\\",?|\\\"102\\\",?|\\\\s', '') != '[]' and core.inv_status<>'402' then concat(core.house_id,case when core.room_id is null then '' else core.room_id end )  else null end )\n" +
//                "                 as off_shelf_stock_cnt                                       --下架库存量\n" +
//                "       ,count(distinct case when core.rent_status = '0' and core.agent_start_date<=current_date() and core.inv_status in ('101') then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as stock_vacancy_rent_started_wait_first_config_cnt          --空置(已起租)待首次配置\n" +
//                "       ,count(distinct case when core.rent_status = '0' and core.agent_start_date<=current_date() and core.inv_status in ('102') then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as stock_vacancy_rent_started_new_hire_config_cnt            --空置(已起租)新收配置中\n" +
//                "       ,count(distinct case when core.rent_status = '0' and core.agent_start_date<=current_date() and core.inv_status in ('201') and core.air_quality in (1,3,4) then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as stock_vacancy_rent_started_wait_rent_air_check_cnt        --空置(已起租)待租空检中\n" +
//                "       ,count(distinct case when core.rent_status = '0' and core.agent_start_date<=current_date() and core.inv_status in ('303') then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as stock_vacancy_rent_started_sec_config_cnt                 --空置(已起租)二次配置中\n" +
//                "       ,count(distinct case when core.rent_status = '0' and core.agent_start_date<=current_date() and core.inv_status in ('201') and core.air_quality in (0, 2) then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as stock_vacancy_rent_started_wait_rent_remove_air_check_cnt --空置(已起租)待租中(不含空检)\n" +
//                "       ,count(distinct case when core.rent_status = '0' and core.agent_start_date<=current_date() and core.inv_status in ('401') then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as stock_vacancy_rent_started_cancel_vacancy_cnt             --空置(已起租)解约空置中\n" +
//                "       ,count(distinct case when core.inv_status in ('203') then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as stock_vacancy_rent_started_cancel_rent_cnt                --空置(已起租)解约在租中\n" +
//                "       ,count(distinct case when core.inv_status in ('203', '401') then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as stock_vacancy_rent_started_cancel_cnt                     --空置(已起租)业主解约中\n" +
//                "       ,count(distinct case when core.rent_status = '0' and core.agent_start_date<=current_date() and core.inv_status<>'402' then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as stock_vacancy_rooms_house_rent_started_cnt                --空置(已起租)\n" +
//                "       ,count(distinct case when core.rent_status = '0' and (core.agent_start_date is null or core.agent_start_date>current_date()) and core.inv_status<>'402' then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as stock_vacancy_rooms_house_no_rent_cnt                     --空置(未起租)\n" +
//                "       ,sum(case when core.rent_status = '0' and core.inv_status<>'402' and core.vacancy_days>100 then 1 else 0 end)\n" +
//                "                 as all_vacancy_days_over_100                                --百日库存\n" +
//                "\t   ,'1' as keeper_type_code\n" +
//                "       ,'所属管家' as keeper_type_name\n" +
//                "       ,trusteeship_type as trusteeship_type_code\n" +
//                "       ,trusteeship_type_name\n" +
//                "       ,core.house_no\n" +
//                "       ,count(distinct case when core.inv_status in ('401') and rent.has_rent is null then concat(core.house_id,case when core.room_id is null then '' else core.room_id end) else null end)\n" +
//                "                 as stock_cancel_before_rent_cnt                     --首次出租前解约\n" +
//                "   from (select manager_code\n" +
//                "                ,resblock_id\n" +
//                "                ,resblock_name\n" +
//                "                ,ziroom_version\n" +
//                "                ,ziroom_version_name\n" +
//                "                ,is_whole\n" +
//                "                ,vacancy_day as vacancy_days\n" +
//                "                ,vacancy_type\n" +
//                "                ,face\n" +
//                "                ,size\n" +
//                "                ,has_balcony\n" +
//                "                ,has_toilet\n" +
//                "                ,room_style_code\n" +
//                "                ,`floor`\n" +
//                "                ,floor_total\n" +
//                "                ,latest_vacancy_time\n" +
//                "                ,agent_start_date\n" +
//                "                ,inv_r_code as room_id\n" +
//                "                ,inv_h_code as house_id\n" +
//                "                ,bedroom_count\n" +
//                "                ,inv_status\n" +
//                "                ,abnormal_code\n" +
//                "                ,air_quality\n" +
//                "                ,trusteeship_type\n" +
//                "                ,trusteeship_type_name\n" +
//                "                ,case when inv_status in ('202', '203') or sale_status = '4' then '1'\n" +
//                "\t                else '0'\n" +
//                "\t            end as rent_status                    --出租状态 1在租, 0空置\n" +
//                "\t            ,house_no\n" +
//                "           from ods.ods_sauron_core_rent_unit_f_m\n" +
//                "          where dt=\"$[yyyyMMdd]$[HHmmss+0/24/60]\"\n" +
//                "            and valid_state=1\n" +
//                "            and inv_status<>'402'\n" +
//                "            --and nvl(ziroom_version,0) <> '10004')core\n" +
//                "            and nvl(ziroom_version,0) not in('1012','1017','1024','1033', '1036', '1037', '1038'))core\n" +
//                "        left join (\n" +
//                "                select\n" +
//                "                    distinct\n" +
//                "                    house_id,\n" +
//                "                    '1' as has_rent -- 有过出租记录\n" +
//                "                from dwd.dwd_rent_contract_detail_f_d\n" +
//                "                where dt = '$[yyyyMMdd-1]'\n" +
//                "                AND is_del=0\n" +
//                "                AND is_only_save=0\n" +
//                "           ) rent on core.house_id = rent.house_id\n" +
//                "           left join (select load_date\n" +
//                "                             ,staff_code\n" +
//                "                             ,staff_name\n" +
//                "                             ,country_name\n" +
//                "                             ,city_code\n" +
//                "                             ,city_name\n" +
//                "                             ,deptid_l2\n" +
//                "                             ,deptid_l2_name\n" +
//                "                             ,deptid_l2_plus\n" +
//                "                             ,deptid_l2_name_plus\n" +
//                "                             ,deptid_l3\n" +
//                "                             ,deptid_l3_name\n" +
//                "                             ,deptid_l4\n" +
//                "                             ,deptid_l4_name\n" +
//                "                             ,deptid_l5\n" +
//                "                             ,deptid_l5_name\n" +
//                "                        from dim.dim_hr_staff_info_f_d hr\n" +
//                "                       where is_main_job=1\n" +
//                "                         and dt='$[yyyyMMdd-1]'\n" +
//                "                         and deptid_l3_name like '%拓展%'\n" +
//                "     ) hr\n" +
//                "      on core.manager_code=hr.staff_code\n" +
//                "   group by\n" +
//                "         hr.country_name\n" +
//                "         ,hr.city_code\n" +
//                "         ,hr.city_name\n" +
//                "         ,hr.deptid_l2\n" +
//                "         ,hr.deptid_l2_name\n" +
//                "         ,hr.deptid_l2_plus\n" +
//                "         ,hr.deptid_l2_name_plus\n" +
//                "         ,hr.deptid_l3\n" +
//                "         ,hr.deptid_l3_name\n" +
//                "         ,hr.deptid_l4\n" +
//                "         ,hr.deptid_l4_name\n" +
//                "         ,hr.deptid_l5\n" +
//                "         ,hr.deptid_l5_name\n" +
//                "         ,core.manager_code\n" +
//                "         ,hr.staff_name\n" +
//                "         ,core.resblock_id\n" +
//                "         ,core.resblock_name\n" +
//                "         ,core.ziroom_version\n" +
//                "         ,core.ziroom_version_name\n" +
//                "         ,case when core.is_whole ='1' then '整租' else '友家' end\n" +
//                "         ,core.bedroom_count\n" +
//                "         ,case when core.is_whole ='0' then '友家'\n" +
//                "               when core.is_whole ='1' and core.bedroom_count=1 then '整租一居'\n" +
//                "               when core.is_whole ='1' and core.bedroom_count>1 then '整租二居及以上'\n" +
//                "             end\n" +
//                "         ,core.vacancy_days\n" +
//                "         ,core.vacancy_type\n" +
//                "         ,core.face\n" +
//                "         ,core.size\n" +
//                "         ,core.has_balcony\n" +
//                "         ,core.has_toilet\n" +
//                "         ,core.room_style_code\n" +
//                "         ,case when core.room_style_code = '38000101' then '主卧'\n" +
//                "               when core.room_style_code = '38000102' then '次卧'\n" +
//                "               when core.room_style_code = '38000103' then '优化间'\n" +
//                "               end\n" +
//                "         ,core.`floor`\n" +
//                "         ,core.floor_total\n" +
//                "         ,case when core.`floor`=1 then '一层'\n" +
//                "               when core.`floor`=core.floor_total then '顶层'\n" +
//                "               end\n" +
//                "         ,trusteeship_type\n" +
//                "         ,trusteeship_type_name\n" +
//                "         ,core.house_no";
//        String sql = "with\n" +
//                "tmp_config_contract as (select a.hire_contract_id\n" +
//                "                               ,hire_contract_code\n" +
//                "                               ,is_goods_share\n" +
//                "                               ,nvl(if(total_config_list_price=0,total_config_cost,total_config_list_price),0) as total_config_list_price\n" +
//                "                               ,nvl(if(broadband_list_price is null,broadband_cost,broadband_list_price),0) as broadband_list_price\n" +
//                "                               ,configuration_type\n" +
//                "                          from (select hire_contract_id\n" +
//                "                                       ,is_goods_share\n" +
//                "                                       ,total_config_list_price\n" +
//                "                                       ,broadband_list_price\n" +
//                "                                       ,configuration_type\n" +
//                "                                       ,total_config_cost\n" +
//                "                                       ,broadband_cost\n" +
//                "                                   from dws.dws_config_each_item_cost_f_d\n" +
//                "                                  where dt='$[yyyyMMdd-1]')a\n" +
//                "                          left join (select hire_contract_id\n" +
//                "                                            ,hire_contract_code\n" +
//                "                                       from dwd.dwd_hire_contract_detail_f_d\n" +
//                "                                      where dt='$[yyyyMMdd-1]') b\n" +
//                "                            on a.hire_contract_id=b.hire_contract_id\n" +
//                "                         ),\n" +
//                "--获取新收或者续约配置成本\n" +
//                "tmp_total_fee as(select ta.hire_contract_code\n" +
//                "                        ,tb.hire_contract_id\n" +
//                "                        ,share_fee / (case when tb.hire_year > 5\n" +
//                "                 \t                     then 60\n" +
//                "                                          else tb.hire_year * 12\n" +
//                "                                           end)\n" +
//                "                 \t    +\n" +
//                "                 \t    unshare_fee / 12 as new_hire_or_renew_month_share_fee\n" +
//                "                        ,ta.total_fee    as new_hire_or_renew_total_fee\n" +
//                "                        ,ta.share_fee    as new_hire_or_renew_total_sharefee\n" +
//                "                        ,ta.unshare_fee  as new_hire_or_renew_total_unsharefee\n" +
//                "                        ,ta.net_fee      as new_hire_or_renew_net_fee\n" +
//                "                   from (select cb.hire_contract_code,\n" +
//                "                                sum(case when cb.is_goods_share = 0\n" +
//                "                 \t\t\t            then total_config_list_price\n" +
//                "                                         else 0\n" +
//                "                                          end) unshare_fee,\n" +
//                "                                sum(case when cb.is_goods_share = 1 then total_config_list_price\n" +
//                "                                         else 0\n" +
//                "                                          end) share_fee,\n" +
//                "                                sum(total_config_list_price) total_fee,\n" +
//                "                                sum(nvl(cb.broadband_list_price, 0)) net_fee\n" +
//                "                           from tmp_config_contract cb\n" +
//                "                          where cb.configuration_type in (0, 1)\n" +
//                "                            and cb.hire_contract_code not like '%-%'\n" +
//                "                          group by cb.hire_contract_code\n" +
//                "\n" +
//                "                          union all\n" +
//                "\n" +
//                "                         select cb.hire_contract_code,\n" +
//                "                                sum(case when cb.is_goods_share = 0\n" +
//                "                 \t\t\t\t        then total_config_list_price\n" +
//                "                                         else 0\n" +
//                "                                          end) unshare_fee,\n" +
//                "                                sum(case when cb.is_goods_share = 1\n" +
//                "                 \t\t\t\t        then total_config_list_price\n" +
//                "                                         else 0\n" +
//                "                                          end) share_fee,\n" +
//                "                                sum(total_config_list_price) total_fee,\n" +
//                "                                sum(nvl(cb.broadband_list_price, 0)) net_fee\n" +
//                "                           from tmp_config_contract cb\n" +
//                "                          where cb.configuration_type in (7, 8)\n" +
//                "                            and cb.hire_contract_code like '%-%'\n" +
//                "                          group by cb.hire_contract_code\n" +
//                "                         ) ta\n" +
//                "\t\t\t\t   join (select hire_contract_id\n" +
//                "                                ,hire_contract_code\n" +
//                "\t\t\t\t\t\t        ,hire_year\n" +
//                "                           from dwd.dwd_hire_contract_detail_f_d\n" +
//                "                          where dt='$[yyyyMMdd-1]') tb\n" +
//                "\t\t\t\t\t on ta.hire_contract_code=tb.hire_contract_code)\n" +
//                "\n" +
//                "--在收房合同明细的基础上增加配置成本\n" +
//                "insert overwrite table dws.dws_hire_contract_f_d partition(dt='$[yyyyMMdd-1]')\n" +
//                "select contract.hire_contract_id                   --合同id\n" +
//                "       ,contract.hire_contract_code                 --收房合同号\n" +
//                "       ,contract.house_id                           --房屋id\n" +
//                "       ,contract.house_code                         --房屋编码\n" +
//                "       ,contract.house_source_code                  --房源编码\n" +
//                "       ,contract.product_version_code               --产品版本code\n" +
//                "\t   ,contract.product_version_name               --产品版本名称\n" +
//                "\t   ,contract.product_line_code                  --产品线code\n" +
//                "\t   ,contract.product_line_name                  --产品线名称\n" +
//                "\t   ,contract.rent_type                          --可出租方式\n" +
//                "       ,contract.payment_cycle_code                 --付款方式,@1:月付,@3:季付,@6:半年付,@12:年付\n" +
//                "\t   ,contract.hire_contract_state_code           --合同签约状态@wqy:未签约,@yqy:已签约,@lyz:”履约中,@ydq:已到期,@yzf:已作废,@ytz:已退租, @jyz:解约中,@yjy:已解约\n" +
//                "       ,contract.hire_contract_state_name           --合同签约状态@wqy:未签约,@yqy:已签约,@lyz:”履约中,@ydq:已到期,@yzf:已作废,@ytz:已退租, @jyz:解约中,@yjy:已解约\n" +
//                "       ,contract.city_code                          --城市code\n" +
//                "\t   ,contract.city_name                          --城市name\n" +
//                "       ,contract.resblock_id                        --楼盘code\n" +
//                "       ,contract.resblock_name                      --楼盘name\n" +
//                "\t   ,contract.before_reform_bedroom_amount       --优化前卧室数量\n" +
//                "       ,contract.before_reform_livingroom_amount    --优化前客厅数量\n" +
//                "       ,contract.after_reform_bedroom_amount        --优化后卧室数量\n" +
//                "       ,contract.after_reform_livingroom_amount     --优化后客厅数量\n" +
//                "       ,contract.after_reform_kitchen_amount        --优化后厨房数量\n" +
//                "       ,contract.after_reform_bathroom_amount       --优化后卫生间数量\n" +
//                "       ,contract.decorate_degree_code               --装修程度code\n" +
//                "       ,contract.decorate_degree_name               --装修程度name\n" +
//                "       ,contract.hire_sign_date                     --合同签约日期\n" +
//                "\t   ,contract.hire_confirm_date                  --业主确认日期（已签约日期）\n" +
//                "\t   ,contract.hire_complete_sign_date            --兼容签约日期和业主确认日期,统计收房量使用\n" +
//                "\t   ,contract.hire_contract_type_code            --合同类型code：1续约，0新签\n" +
//                "\t   ,contract.hire_contract_type_name            --合同类型name：1续约，0新签\n" +
//                "\t   ,contract.hire_channel_code                  --收房渠道代码：1续约，0渠道，2综合管家直收，3直收管家直收\n" +
//                "\t   ,contract.hire_channel_name                  --收房渠道名称：1续约，0渠道，2综合管家直收，3直收管家直收\n" +
//                "       ,contract.hire_year                          --签约年\n" +
//                "       ,contract.hire_month                         --签约月\n" +
//                "       ,contract.hire_month_price                   --收房签约月租金\n" +
//                "       ,contract.current_hire_month_price           --实际收房签约月租金(有涨幅)\n" +
//                "       ,contract.hire_start_date                   --合同起租日\n" +
//                "       ,contract.hire_end_date                     --合同截止日\n" +
//                "\t   ,contract.accept_time                        --竣工验收日期\n" +
//                "\t   ,contract.amount_house_date                  --量房日期\n" +
//                "\t   ,config.new_hire_or_renew_total_fee\t       --总配置费用\n" +
//                "       ,config.new_hire_or_renew_total_sharefee   --分摊费用总和\n" +
//                "       ,config.new_hire_or_renew_total_unsharefee --不分摊费用综合\n" +
//                "       ,config.new_hire_or_renew_month_share_fee  --月分摊成本\n" +
//                "       ,config.new_hire_or_renew_net_fee          --宽带费用\n" +
//                "\t   ,contract.old_hire_contract_id               --上份合同ID\n" +
//                "       ,contract.owner_id                           --业主id\n" +
//                "       ,contract.house_service_keeper_code          --服务管家系统号\n" +
//                "       ,contract.house_service_keeper_name          --服务管家姓名\n" +
//                "       ,contract.house_belong_keeper_code           --房屋所属管家系统号\n" +
//                "       ,contract.house_belong_keeper_name           --房屋所属管家姓名\n" +
//                "       ,contract.hire_keeper_code                   --自如管家系统号\n" +
//                "       ,contract.hire_keeper_name                   --自如管家姓名\n" +
//                "       ,contract.is_direct_har                      --业主来源\n" +
//                "       ,contract.audit_state                        --合同签约状态,@-1:未提交审核,@0:待审核,@1:审核未通过,@2:审核通过\n" +
//                "       ,contract.audit_date                         --审核通过时间\n" +
//                "       ,contract.valuation_model_id                 --计价模型id\n" +
//                "       ,contract.property_type                      --产权类型\n" +
//                "       ,contract.biz_opp_id                         --商机id\n" +
//                "       ,contract.hire_conf_staff_code               --收房配置专员系统号\n" +
//                "       ,contract.hire_conf_staff_name               --收房配置专员姓名\n" +
//                "       ,contract.hire_belong_conf_staff_code        --所属配置专员系统号\n" +
//                "       ,contract.hire_belong_conf_staff_name        --所属配置专员姓名\n" +
//                "       ,contract.is_del                             --是否删除,1是,0否\n" +
//                "\t   ,contract.is_only_save                       --是否是草稿,1是,0否\n" +
//                "       ,contract.create_date                        --创建日期\n" +
//                "       ,contract.last_modify_time                   --更新日期\n" +
//                "       ,contract.data_source_system                 --数据系统来源'\n" +
//                "       ,contract.hand_over_date\n" +
//                "       ,contract.trusteeship_type_code              --托管模式code\n" +
//                "       ,contract.trusteeship_type_name              --托管模式name\n" +
//                "  from (select hire_contract_id                   --合同id\n" +
//                "               ,hire_contract_code                 --收房合同号\n" +
//                "               ,house_id                           --房屋id\n" +
//                "               ,house_code                         --房屋编码\n" +
//                "               ,house_source_code                  --房源编码\n" +
//                "               ,product_version_code               --产品版本code\n" +
//                "\t           ,product_version_name               --产品版本名称\n" +
//                "\t           ,product_line_code                  --产品线code\n" +
//                "\t           ,product_line_name                  --产品线名称\n" +
//                "\t           ,rent_type                          --可出租方式\n" +
//                "               ,payment_cycle_code                 --付款方式,@1:月付,@3:季付,@6:半年付,@12:年付\n" +
//                "\t           ,hire_contract_state_code           --合同签约状态@wqy:未签约,@yqy:已签约,@lyz:”履约中,@ydq:已到期,@yzf:已作废,@ytz:已退租, @jyz:解约中,@yjy:已解约\n" +
//                "               ,hire_contract_state_name           --合同签约状态@wqy:未签约,@yqy:已签约,@lyz:”履约中,@ydq:已到期,@yzf:已作废,@ytz:已退租, @jyz:解约中,@yjy:已解约\n" +
//                "               ,city_code                          --城市code\n" +
//                "\t           ,city_name                          --城市name\n" +
//                "               ,resblock_id                        --楼盘code\n" +
//                "               ,resblock_name                      --楼盘name\n" +
//                "\t           ,before_reform_bedroom_amount       --优化前卧室数量\n" +
//                "               ,before_reform_livingroom_amount    --优化前客厅数量\n" +
//                "               ,after_reform_bedroom_amount        --优化后卧室数量\n" +
//                "               ,after_reform_livingroom_amount     --优化后客厅数量\n" +
//                "               ,after_reform_kitchen_amount        --优化后厨房数量\n" +
//                "               ,after_reform_bathroom_amount       --优化后卫生间数量\n" +
//                "               ,decorate_degree_code               --装修程度code\n" +
//                "               ,decorate_degree_name               --装修程度name\n" +
//                "               ,hire_sign_date                     --合同签约日期\n" +
//                "\t           ,hire_confirm_date                  --业主确认日期（已签约日期）\n" +
//                "\t           ,hire_complete_sign_date            --兼容签约日期和业主确认日期,统计收房量使用\n" +
//                "\t           ,hire_contract_type_code            --合同类型code：1续约，0新签\n" +
//                "\t           ,hire_contract_type_name            --合同类型name：1续约，0新签\n" +
//                "\t           ,hire_channel_code                  --收房渠道代码：1续约，0渠道，2综合管家直收，3直收管家直收\n" +
//                "\t           ,hire_channel_name                  --收房渠道名称：1续约，0渠道，2综合管家直收，3直收管家直收\n" +
//                "               ,hire_year                          --签约年\n" +
//                "               ,hire_month                         --签约月\n" +
//                "               ,hire_month_price                   --收房签约月租金\n" +
//                "               ,current_hire_month_price           --实际收房签约月租金(有涨幅)\n" +
//                "               ,hire_start_date                   --合同起租日\n" +
//                "               ,hire_end_date                     --合同截止日\n" +
//                "\t           ,accept_time                        --竣工验收日期\n" +
//                "\t           ,amount_house_date                  --量房日期\n" +
//                "\t           ,old_hire_contract_id               --上份合同ID\n" +
//                "               ,owner_id                           --业主id\n" +
//                "               ,house_service_keeper_code          --服务管家系统号\n" +
//                "               ,house_service_keeper_name          --服务管家姓名\n" +
//                "               ,house_belong_keeper_code           --房屋所属管家系统号\n" +
//                "               ,house_belong_keeper_name           --房屋所属管家姓名\n" +
//                "               ,hire_keeper_code                   --自如管家系统号\n" +
//                "               ,hire_keeper_name                   --自如管家姓名\n" +
//                "               ,is_direct_har                      --业主来源\n" +
//                "               ,audit_state                        --合同签约状态,@-1:未提交审核,@0:待审核,@1:审核未通过,@2:审核通过\n" +
//                "               ,audit_date                         --审核通过时间\n" +
//                "               ,valuation_model_id                 --计价模型id\n" +
//                "               ,property_type                      --产权类型\n" +
//                "               ,biz_opp_id                         --商机id\n" +
//                "               ,hire_conf_staff_code               --收房配置专员系统号\n" +
//                "               ,hire_conf_staff_name               --收房配置专员姓名\n" +
//                "               ,hire_belong_conf_staff_code        --所属配置专员系统号\n" +
//                "               ,hire_belong_conf_staff_name        --所属配置专员姓名\n" +
//                "               ,is_del                             --是否删除,1是,0否\n" +
//                "\t           ,is_only_save                       --是否是草稿,1是,0否\n" +
//                "               ,create_date                        --创建日期\n" +
//                "               ,last_modify_time                   --更新日期\n" +
//                "               ,data_source_system                 --数据系统来源'\n" +
//                "               ,hand_over_date\n" +
//                "               ,trusteeship_type_code              --托管模式code\n" +
//                "               ,trusteeship_type_name              --托管模式name\n" +
//                "          from dwd.dwd_hire_contract_detail_f_d\n" +
//                "\t\t where dt='$[yyyyMMdd-1]') contract\n" +
//                "  left join tmp_total_fee config\n" +
//                "    on contract.hire_contract_id = config.hire_contract_id\n";
        ParseDriver pd = new ParseDriver();
        try {
            ASTNode tree = pd.parse(sql);
            System.out.println(tree.dump());
            System.out.println(tree.toStringTree());
            HiveSqlParser hiveSqlParser = new HiveSqlParser();
            hiveSqlParser.sqlParser(sql);
            System.out.println(JSON.toJSONString(hiveSqlParser.outputTableList));
            System.out.println(JSON.toJSONString(hiveSqlParser.tableList));

//            tree.dupNode()
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    public void sqlParser(String sql) throws ParseException {
        if (StringUtils.isBlank(sql)) {
            return;
        }

        ParseDriver parseDriver = new ParseDriver();
        ASTNode tree = parseDriver.parse(sql);

        // 递归树
        recursionTree(tree, sql);
    }

    private void recursionTree(ASTNode tree, String sql) {
        List<Node> treeChildren = tree.getChildren();

        if (treeChildren == null || CollectionUtils.isEmpty(treeChildren)) {
            return;
        }

        for (Node treeChild : treeChildren) {
            // 只处理抽象语法树节点
            if (treeChild instanceof ASTNode) {
                ASTNode treeChildAstNode = (ASTNode) treeChild;
                // 获取节点token类型
                Integer nodeType = treeChildAstNode.getType();
                // 忽略token类型为null节点（根节点）
                if (nodeType == null) {
                    return;
                }

                // 忽略EOF
                if (HiveParser.EOF == nodeType.intValue()) {
                    return;
                }

                // 当前节点为以下类型，继续递归：查询语句、查询源、表查询
//                if (HiveParser.TOK_QUERY == nodeType.intValue()
//                        || HiveParser.TOK_FROM == nodeType.intValue()
//                        || HiveParser.TOK_TABREF == nodeType.intValue()
//                ) {
//                    recursionTree(treeChildAstNode);
//                }

                // 输出表分区
                if (HiveParser.TOK_PARTVAL == nodeType.intValue()) {
                    wrapOutputTable(treeChildAstNode);
                }
                // 修改、删除表
                else if (HiveParser.TOK_UPDATE_TABLE == nodeType.intValue()
                        || HiveParser.TOK_DELETE_FROM == nodeType.intValue()
                        || HiveParser.TOK_TABREF == nodeType.intValue()
                        || HiveParser.TOK_CREATETABLE == nodeType.intValue()
                        || HiveParser.TOK_DROPTABLE == nodeType.intValue()) {
                    wrapTable(treeChildAstNode, OperationType.getOperationType(nodeType.intValue()), sql);
                } else if (HiveParser.TOK_INSERT == nodeType.intValue()) {
                    wrapInsertTable(treeChildAstNode, OperationType.getOperationType(nodeType.intValue()), sql);
                } else if (HiveParser.TOK_CTE == nodeType.intValue()) {
                    // 临时表
                    wrapTempTable(treeChildAstNode, OperationType.getOperationType(nodeType.intValue()), sql);
                }
                // 分区名称
//                else if (HiveParser.TOK_PARTSPEC == nodeType.intValue()) {
//                    wrapTableNameNode(treeChildAstNode);
//                }
//                else {
//                    // 其他节点则继续递归
//                    recursionTree(treeChildAstNode);
//                }

                // 继续递归
                recursionTree(treeChildAstNode, sql);
            }
        }
    }

    private void wrapTempTable(ASTNode treeChildAstNode, OperationType operationType, String sql) {
        List<Node> treeChildrenList = treeChildAstNode.getChildren();
        if (CollectionUtils.isEmpty(treeChildrenList)) {
            return;
        }

        for (Node node : treeChildrenList) {
            if (node instanceof ASTNode) {
                ASTNode astNode = (ASTNode) node;
                // 获取节点token类型
                Integer nodeType = astNode.getType();
                if (nodeType == HiveParser.TOK_SUBQUERY) {
                    List<Node> subQueryNodes = astNode.getChildren();
                    if (CollectionUtils.isEmpty(subQueryNodes)) {
                        return;
                    }
                    for (Node subQuery : subQueryNodes) {
                        if (subQuery instanceof ASTNode) {
                            ASTNode subQueryAstNode = (ASTNode) subQuery;
                            if (subQueryAstNode.getType() == HiveParser.Identifier) {
                                this.tempTableList.add(new Table(null, subQueryAstNode.getText(), operationType, sql));
                            }
                        }
                    }
                }

            }
        }
    }

    private void wrapInsertTable(ASTNode treeChildAstNode, OperationType operationType, String sql) {
        List<Node> treeChildrenList = treeChildAstNode.getChildren();
        if (CollectionUtils.isEmpty(treeChildrenList)) {
            return;
        }

        for (Node node : treeChildrenList) {
            if (node instanceof ASTNode) {
                ASTNode astNode = (ASTNode) node;
                // 获取节点token类型
                Integer nodeType = astNode.getType();
                if (nodeType == HiveParser.TOK_DESTINATION) {
                    List<Node> destinationNodes = astNode.getChildren();
                    if (CollectionUtils.isEmpty(destinationNodes)) {
                        return;
                    }
                    for (Node destination : destinationNodes) {
                        if (destination instanceof ASTNode) {
                            ASTNode destinationAstNode = (ASTNode) destination;

                            if (destinationAstNode.getType() == HiveParser.TOK_TAB) {
                                List<Node> tabNodeList = destinationAstNode.getChildren();
                                if (CollectionUtils.isEmpty(tabNodeList)) {
                                    return;
                                }
                                wrapTable(destinationAstNode, operationType, sql);
                            }
                        }
                    }
                }
            }

        }
    }

    private void wrapTable(ASTNode treeChildAstNode, OperationType operationType, String sql) {
        List<Node> treeChildrenList = treeChildAstNode.getChildren();
        if (CollectionUtils.isEmpty(treeChildrenList)) {
            return;
        }

        for (Node node : treeChildrenList) {
            if (node instanceof ASTNode) {
                ASTNode astNode = (ASTNode) node;
                // 获取节点token类型
                Integer nodeType = astNode.getType();
                if (nodeType == HiveParser.TOK_TABNAME) {
                    List<Node> tableNameNodes = astNode.getChildren();
                    if (CollectionUtils.isEmpty(tableNameNodes)) {
                        return;
                    }
                    String dbName = null;
                    String tableName = null;

                    if (tableNameNodes.size() < 2) {
                        // 未包含库名
                        Node tableNameNode = tableNameNodes.get(0);
                        if (tableNameNode instanceof ASTNode) {
                            tableName = ((ASTNode) tableNameNode).getText();
                        }
                    } else {
                        // 包含库名.表名
                        Node dbNameNode = tableNameNodes.get(0);
                        if (dbNameNode instanceof ASTNode) {
                            dbName = ((ASTNode) dbNameNode).getText();
                        }
                        Node tableNameNode = tableNameNodes.get(1);
                        if (tableNameNode instanceof ASTNode) {
                            tableName = ((ASTNode) tableNameNode).getText();
                        }
                    }
                    this.tableList.add(new Table(dbName, tableName, operationType, sql));
                }
            }

        }
    }


    private void wrapOutputTable(ASTNode treeChildAstNode) {
        List<Node> tableNameNodes = treeChildAstNode.getChildren();
        if (CollectionUtils.isEmpty(tableNameNodes) || tableNameNodes.size() != 2) {
            return;
        }
        String partitionName = null;
        String partitionValue = null;

        Node dbNameNode = tableNameNodes.get(0);
        if (dbNameNode instanceof ASTNode) {
            partitionName = ((ASTNode) dbNameNode).getText();
        }
        Node tableNameNode = tableNameNodes.get(1);
        if (tableNameNode instanceof ASTNode) {
            partitionValue = ((ASTNode) tableNameNode).getText();
        }
        this.outputTableList.add(new OutputTable(partitionName, partitionValue));
    }

//    private void wrapTableNameNode(ASTNode treeChildAstNode) {
//        List<Node> tableNameNodes = treeChildAstNode.getChildren();
//        if (CollectionUtils.isNotEmpty(tableNameNodes)) {
//            String dbName = null;
//            String tableName = null;
//            if (tableNameNodes.size() == 2) {
//                Node dbNameNode = tableNameNodes.get(0);
//                if (dbNameNode instanceof ASTNode) {
//                    dbName = ((ASTNode) dbNameNode).getText();
//                }
//                Node tableNameNode = tableNameNodes.get(1);
//                if (tableNameNode instanceof ASTNode) {
//                    tableName = ((ASTNode) tableNameNode).getText();
//                }
//            } else if (tableNameNodes.size() == 1) {
//                Node tableNameNode = tableNameNodes.get(0);
//                if (tableNameNode instanceof ASTNode) {
//                    tableName = ((ASTNode) tableNameNode).getText();
//                }
//            }
//            inputTableList.add(new InputTable(dbName, tableName, null, null));
//        }
//    }

    private Integer getTokenType(ASTNode treeChildAstNode) {
        return Optional.ofNullable(treeChildAstNode)
                .map(e -> e.getToken())
                .filter(e -> e != null)
                .map(e -> e.getType())
                .orElse(null);
    }

    public enum OperationType {
        CREATE("CREATE"), DROP("DROP"), INSERT("INSERT"), DELETE("DELETE"), UPDATE("UPDATE"), SELECT("SELECT"), TEMP("TEMP");

        private String tableType;

        OperationType(String tableType) {
            this.tableType = tableType;
        }

        public String getTableType() {
            return tableType;
        }

        public static OperationType getOperationType(int hiveParserTok) {
            switch (hiveParserTok) {
                case HiveParser.TOK_INSERT:
                case HiveParser.TOK_INSERT_INTO:
                    return INSERT;
                case HiveParser.TOK_DELETE_FROM:
                    return DELETE;
                case HiveParser.TOK_UPDATE_TABLE:
                    return UPDATE;
                case HiveParser.TOK_TABREF:
                    return SELECT;
                case HiveParser.TOK_CREATETABLE:
                    return CREATE;
                case HiveParser.TOK_DROPTABLE:
                    return DROP;
                case HiveParser.TOK_CTE:
                    return TEMP;
                default:
                    return null;
            }

        }
    }


    public static class Table {
        /**
         * 库名
         */
        private String dbName;
        /**
         * 表名
         */
        private String tableName;

        /**
         * ddl类型
         *
         * @see OperationType
         */
        private OperationType OperationType;

        /**
         * 所属sql
         */
        private String sql;

        public Table(String dbName, String tableName, OperationType operationType, String sql) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.OperationType = operationType;
            this.sql = sql;
        }

        public Table() {
        }

        public String getDbName() {
            return dbName;
        }

        public void setDbName(String dbName) {
            this.dbName = dbName;
        }

        public String getTableName() {
            return tableName;
        }

        public HiveSqlParser.OperationType getOperationType() {
            return OperationType;
        }

        public String getSql() {
            return sql;
        }
    }


    public static class InputTable {
        /**
         * 库名
         */
        private String dbName;
        /**
         * 表名
         */
        private String tableName;
        /**
         * 分区列
         */
        private String partitionColumn;
        /**
         * 分区值
         */
        private String partitionValue;

        public InputTable() {
        }

        public InputTable(String dbName, String tableName, String partitionColumn, String partitionValue) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.partitionColumn = partitionColumn;
            this.partitionValue = partitionValue;
        }

        public String getDbName() {
            return dbName;
        }

        public void setDbName(String dbName) {
            this.dbName = dbName;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getPartitionColumn() {
            return partitionColumn;
        }

        public void setPartitionColumn(String partitionColumn) {
            this.partitionColumn = partitionColumn;
        }

        public String getPartitionValue() {
            return partitionValue;
        }

        public void setPartitionValue(String partitionValue) {
            this.partitionValue = partitionValue;
        }
    }

    public static class OutputTable {
        /**
         * 库名
         */
//        private String dbName;
        /**
         * 表名
         */
//        private String tableName;
        /**
         * 分区列
         */
        private String partitionColumn;
        /**
         * 分区值
         */
        private String partitionValue;

        public OutputTable(String partitionColumn, String partitionValue) {
            this.partitionColumn = partitionColumn;
            this.partitionValue = partitionValue;
        }

        public OutputTable() {
        }

        public String getPartitionColumn() {
            return partitionColumn;
        }

        public void setPartitionColumn(String partitionColumn) {
            this.partitionColumn = partitionColumn;
        }

        public String getPartitionValue() {
            return partitionValue;
        }

        public void setPartitionValue(String partitionValue) {
            this.partitionValue = partitionValue;
        }
    }

//    public List<InputTable> getInputTableList() {
//        return inputTableList;
//    }

    public List<OutputTable> getOutputTableList() {
        return outputTableList;
    }

    public List<Table> getTableList() {
        return tableList;
    }

    public List<Table> getTempTableList() {
        return tempTableList;
    }
}
