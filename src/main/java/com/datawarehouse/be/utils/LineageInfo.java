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

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;
import java.util.TreeSet;
public class LineageInfo implements NodeProcessor {
    TreeSet<String> inputTableList = new TreeSet();
    TreeSet<String> OutputTableList = new TreeSet();
    TreeSet<String> withTableList = new TreeSet<>();

    public LineageInfo() {
    }

    public TreeSet<String> getInputTableList() {
        return this.inputTableList;
    }

    public TreeSet<String> getOutputTableList() {
        return this.OutputTableList;
    }

    public TreeSet<String> getWithTableList() {
        return withTableList;
    }

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        ASTNode pt = (ASTNode) nd;
        switch (pt.getToken().getType()) {
            case HiveParser.TOK_TAB:
                this.OutputTableList.add(BaseSemanticAnalyzer.getUnescapedName((ASTNode) pt.getChild(0)));
                break;
            case HiveParser.TOK_TABREF:
                ASTNode tabTree = (ASTNode) pt.getChild(0);
                String table_name = tabTree.getChildCount() == 1 ? BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0)) + "." + tabTree.getChild(1);
                this.inputTableList.add(table_name);
                break;
            case HiveParser.TOK_CTE:
                // with.....语句
                for (int i = 0, j = pt.getChildCount(); i < j; i++) {
                    ASTNode temp = (ASTNode) pt.getChild(i);
                    String cteName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) temp.getChild(1));
                    withTableList.add(cteName);
                }
                break;
        }

        return null;
    }

    public void getLineageInfo(String query) throws ParseException, SemanticException {
//        ASTNode tree;
//        for(tree = ParseUtils.parse(query, (Context)null); tree.getToken() == null && tree.getChildCount() > 0; tree = (ASTNode)tree.getChild(0)) {
//        }
//
//        this.inputTableList.clear();
//        this.OutputTableList.clear();
//        Map<Rule, NodeProcessor> rules = new LinkedHashMap();
//        Dispatcher disp = new DefaultRuleDispatcher(this, rules, (NodeProcessorCtx)null);
//        GraphWalker ogw = new DefaultGraphWalker(disp);
//        ArrayList<Node> topNodes = new ArrayList();
//        topNodes.add(tree);
//        ogw.startWalking(topNodes, (HashMap)null);

        /*
         * Get the AST tree
         */
        ASTNode tree = ParseUtils.parse(query, null);

//        ParseDriver pd = new ParseDriver();
//        ASTNode tree = pd.parseExpression(query);

//        HiveConf hiveConf = new HiveConf();
//        Context context = new Context(hiveConf);
//        ASTNode tree = ParseUtils.parse(query, context);

        while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
            tree = (ASTNode) tree.getChild(0);
        }

        /*
         * initialize Event Processor and dispatcher.
         */
        inputTableList.clear();
        OutputTableList.clear();
        withTableList.clear();

        // create a walker which walks the tree in a DFS manner while maintaining
        // the operator stack. The dispatcher
        // generates the plan from the operator tree
        Map<Rule, NodeProcessor> rules = new LinkedHashMap<Rule, NodeProcessor>();

        // The dispatcher fires the processor corresponding to the closest matching
        // rule and passes the context along
        Dispatcher disp = new DefaultRuleDispatcher(this, rules, null);
        GraphWalker ogw = new DefaultGraphWalker(disp);

        // Create a list of topop nodes
        ArrayList<Node> topNodes = new ArrayList<Node>();
        topNodes.add(tree);
        ogw.startWalking(topNodes, null);
    }

    public static void main(String[] args) throws IOException, ParseException, SemanticException {
//        String query = args[0];
//        String query = "with\n" +
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
//        String query = "select name, code from testdb.test;-- 注释";
//        String query = "INSERT OVERWRITE TABLE test.bz_user partition(dt='$[yyyyMMdd-1]') \n" +
//                "select code, name from test.dep where dt='$[yyyyMMdd-1]'";
//        String query = "select `中文列名` from db.tablename;";
//        String query = "select 中文列名 from db.tablename;";
//        String query = "select code, name, class as `中文列名称` from db.tablename";
        String query = "select\n" +
                "  after_bedroom 飘号,\n" +
                "  before_bedroom as `before_bedroom1`,\n" +
                "  biz_bedroom ,\n" +
                "  biz_is_first_follow_timeout ,\n" +
                "  biz_keeper_rating ,\n" +
                "  biz_product_line_code ,\n" +
                "  biz_product_line_name ,\n" +
                "  biz_product_version_code ,\n" +
                "  biz_product_version_name ,\n" +
                "  biz_rating ,\n" +
                "  biz_rent_type ,\n" +
                "  `biz_trusteeship_type_code` ,\n" +
                "  `biz_trusteeship_type_name` ,\n" +
                "  `city_name` ,\n" +
                "  `city_name_house` ,\n" +
                "  case city_name_house\n" +
                "    WHEN '北京' THEN  '110000'\n" +
                "    WHEN '深圳' THEN  '440300'\n" +
                "    WHEN '上海' THEN  '310000'\n" +
                "    WHEN '南京' THEN  '320100'\n" +
                "    WHEN '杭州' THEN  '330100'\n" +
                "    WHEN '武汉' THEN  '420100'\n" +
                "    WHEN '广州' THEN  '440100'\n" +
                "    WHEN '成都' THEN  '510100'\n" +
                "    WHEN '天津' THEN  '120000'\n" +
                "    WHEN '苏州' THEN  '320500'\n" +
                "    END ,\n" +
                "  `cnt` ,\n" +
                "  `cnt_type` ,\n" +
                "  `daqu_name` ,\n" +
                "  `datetime` ,\n" +
                "  `deptid_l4` ,\n" +
                "  `deptid_l5` ,\n" +
                "  `job_code_desc` ,\n" +
                "  `keeper_code`,\n" +
                "  `product_line_code` ,\n" +
                "  `product_line_name` ,\n" +
                "  `product_version_code` ,\n" +
                "  `product_version_name` ,\n" +
                "  `rent_type` ,\n" +
                "  `resblock_id` ,\n" +
                "  `resblock_name` ,\n" +
                "  `resblock_rating_highest`,\n" +
                "  `source_one_name` ,\n" +
                "  `source_two_name` ,\n" +
                "  `staff_code` ,\n" +
                "  `staff_name` ,\n" +
                "  `trusteeship_type_code` ,\n" +
                "  `trusteeship_type_name` ,\n" +
                "  `yewuzu_name` ,\n" +
                "  `zhongxin_name` \n" +
                "from\n" +
                "  oi.oi_indicator_hire_cross_sectional_f_d\n" +
                "where\n" +
                "  dt='$[yyyyMMdd]'\n" +
                "  and date(datetime)>=trunc(add_months(current_date,-3),'MM')";
        LineageInfo lep = new LineageInfo();
        lep.getLineageInfo(query);
        Iterator var3 = lep.getInputTableList().iterator();

        String tab;
        while (var3.hasNext()) {
            tab = (String) var3.next();
            System.out.println("InputTable=" + tab);
        }

        var3 = lep.getOutputTableList().iterator();

        while (var3.hasNext()) {
            tab = (String) var3.next();
            System.out.println("OutputTable=" + tab);
        }

        var3 = lep.getWithTableList().iterator();
        while (var3.hasNext()) {
            tab = (String) var3.next();
            System.out.println("WithTable=" + tab);
        }

    }
}
