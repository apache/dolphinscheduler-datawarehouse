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

package com.datawarehouse.be.pojo.params;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.datawarehouse.be.exceptions.SQLIOException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

@Data
@Api(tags = "SqlParsingParam")
public class SqlParsingParam {

    /**
     * process Id
     */
    @ApiModelProperty(name = "processId", value = "工作流Id", dataType = "java.lang.Integer")
    private Long processId = null;

    /**
     * process Id
     */
    @ApiModelProperty(name = "Sql", value = "工作流SQL", dataType = "java.lang.String", example = "INSERT OVERWRITE TABLE test.opentsdb partition(dt='$[yyyyMMdd-1]')\n" +
            "select t.rent_contract_code from dwd.dwd_rent_contract_detail_f_d t where t.dt = '$[yyyyMMdd-1]';")
    private String sql;

    public void verify(){
        if (StringUtils.isBlank(sql)) {
            throw new SQLIOException("解析SQL，入参sql不能为空，请填写后重试~");
        }
    }

    public static void main(String[] args) {
        JSONObject json = new JSONObject();
        json.put("processId", null);
        json.put("sql", "CREATE TABLE IF NOT EXISTS bl_bi.pj_all_in_2021_lvzy(\n" +
                "\n" +
                "t_collect_id STRING comment '评价主键',\n" +
                "t_be_evaluator_type STRING comment '受评对象类型',\n" +
                "t_be_evaluator_name STRING comment '受评对象名称',\n" +
                "pz_order_code STRING comment '房屋的配置单号',\n" +
                "t_score_flag STRING comment '问题类型编号',\n" +
                "t_label STRING comment '选项对应的内容',\n" +
                "object_code STRING comment '被评价人编号',\n" +
                "group_manager_code STRING comment '管家经理编码',\n" +
                "group_manager_name STRING comment '管家经理名称',\n" +
                "dept_manager_code STRING comment '管家总监编码',\n" +
                "dept_manager_name STRING comment '管家总监名称',\n" +
                "t_evaluate_date STRING comment '评价日期',\n" +
                "order_code STRING comment '业务单号',\n" +
                "t_be_evaluator_id STRING comment '受评对象ID',\n" +
                "t_question_type STRING comment '问题类型',\n" +
                "t_name STRING comment '问题类型名称',\n" +
                "t_question_value STRING comment '问题得分',\n" +
                "t_question_option_id STRING comment '问题',\n" +
                "t_label_od STRING comment '选项对应的内容',\n" +
                "t_question_content STRING comment '填写型问题答案',\n" +
                "final_star_score STRING comment '星级',\n" +
                "template_code STRING comment '模板code',\n" +
                "template_name STRING comment '模板名称',\n" +
                "question_type STRING comment '问题类型',\n" +
                "supplier_name STRING comment '维修供应商名称',\n" +
                "t_request_evaluator_id STRING comment '客户ID',\n" +
                "city_code STRING comment '城市代码',\n" +
                "city_general_manager_staff_name STRING comment '城市总经理名称',\n" +
                "center_general_manager_staff_name STRING comment '战区总经理名称',\n" +
                "staff_code STRING comment '工号',\n" +
                "staff_name STRING comment '姓名',\n" +
                "\n" +
                "deptid_name STRING comment '部门描述',\n" +
                "deptid_l1_name STRING comment '一级部门描述',\n" +
                "deptid_l2_name STRING comment '二级部门描述',\n" +
                "deptid_l3_name STRING comment '三级部门描述,战区name',\n" +
                "deptid_l4_name STRING comment '四级部门描述,大区name',\n" +
                "deptid_l5_name STRING comment '五级部门描述,业务组name',\n" +
                "deptid_l6_name STRING comment '六级部门描述',\n" +
                "deptid_l7_name STRING comment '七级部门描述',\n" +
                "job_position_desc STRING comment '职位描述',\n" +
                "job_code_desc STRING comment '职务描述',\n" +
                "job_grade_desc STRING comment '职等描述',\n" +
                "job_level STRING comment '职级',\n" +
                "is_resign STRING comment '是否离职：0在职，1离职',\n" +
                "join_ziroom_date STRING comment '入自如日期',\n" +
                "manager_staff_name STRING comment '经理名称',\n" +
                "director_staff_name STRING comment '总监名称',\n" +
                "room_id STRING comment '房间ID',\n" +
                "room_code STRING comment '房间编号',\n" +
                "room_name STRING comment '房间名称',\n" +
                "rent_contract_type_name STRING comment '合同类型',\n" +
                "service_keeper_code STRING comment '所属服务管家',\n" +
                "is_rent STRING comment '是否在租 是1 否0',\n" +
                "is_first_rent STRING comment '是否首次',\n" +
                "room_type_name STRING comment '房间类型名称',\n" +
                "product_line_name STRING comment '产品线名称',\n" +
                "decorate_degree_name STRING comment '装修程度name',\n" +
                "rent_contract_code STRING comment '出房合同号',\n" +
                "rent_sign_date STRING comment '出房签约日期',\n" +
                "rent_start_date STRING comment '出房起租日期',\n" +
                "product_version_code STRING comment '产品版本code',\n" +
                "resblock_id STRING comment '楼盘code',\n" +
                "resblock_name STRING comment '楼盘name',\n" +
                "after_reform_bedroom_amount STRING comment '优化后卧室数量',  \n" +
                "hire_contract_type_name STRING comment '合同类型name：1续约，0新签',\n" +
                "accept_time STRING comment '竣工验收日期',\n" +
                "old_hire_contract_id STRING comment '上份合同ID',\n" +
                "hire_conf_staff_code STRING comment '收房配置专员系统号',\n" +
                "hire_belong_conf_staff_code STRING comment '所属配置专员系统号',\n" +
                "hire_contract_code STRING comment '收房合同号',\n" +
                "house_belong_keeper_code STRING comment '房屋所属管家系统号',\n" +
                "hire_start_date STRING comment '合同起租日',\n" +
                "configuration_type STRING comment '配置类型 0:新收标准配珠 1:新收补充配置 2:租期配置',\n" +
                "staff_name_jl STRING comment '姓名(经理绩效)',\n" +
                "city_name_jl STRING comment '城市(经理绩效)',\n" +
                "deptid_l3_name_jl STRING comment '战区(经理绩效)',\n" +
                "deptid_l4_name_jl STRING comment '业务部(经理绩效)',\n" +
                "deptid_l5_name_jl STRING comment '业务组(经理绩效)',\n" +
                "staff_name_zj STRING comment '姓名(总监绩效)',\n" +
                "city_name_zj STRING comment '城市(总监绩效)',\n" +
                "deptid_l3_name_zj STRING comment '战区(总监绩效)',\n" +
                "deptid_l4_name_zj STRING comment '业务部(总监绩效)'\n" +
                ")\n" +
                "COMMENT '评价数据连一切'\n" +
                "PARTITIONED BY (dt string COMMENT 'format is yyyyMMdd')\n" +
                "STORED AS ORC\n" +
                "tblproperties('BUSINESS_OWNER'='shiym1', 'DATA_OWNER'='shiym1' ,'TAGS'='pj_all_in_2021_lvzy',\n" +
                "\n" +
                "'partition.retention.period' = '90d', 'discover.partitions' = 'true' ); \n" +
                "\n" +
                "\n" +
                "INSERT OVERWRITE TABLE bl_bi.pj_all_in_2021_lvzy PARTITION ( dt ='$[yyyyMMdd-1]')\n" +
                "\n" +
                "select \n" +
                "       \n" +
                "       t1_t_collect_id as t_collect_id,\n" +
                "       t1_t_be_evaluator_type as t_be_evaluator_type,\n" +
                "       t1_t_be_evaluator_name as t_be_evaluator_name,\n" +
                "       t1_pz_order_code as pz_order_code,\n" +
                "       t1_t_score_flag as t_score_flag,\n" +
                "       t1_t_label as t_label,\n" +
                "       t1_object_code as object_code,\n" +
                "       t1_group_manager_code as group_manager_code,\n" +
                "       t1_group_manager_name as group_manager_name,\n" +
                "       t1_dept_manager_code as dept_manager_code,\n" +
                "       t1_dept_manager_name as dept_manager_name,\n" +
                "       t1_t_evaluate_date as t_evaluate_date,\n" +
                "       t1_order_code as order_code,\n" +
                "       t1_t_be_evaluator_id as t_be_evaluator_id,\n" +
                "       t1_t_question_type as t_question_type,\n" +
                "       t1_t_name as t_name,\n" +
                "       t1_t_question_value as t_question_value,\n" +
                "       t1_t_question_option_id as t_question_option_id,\n" +
                "       t1_t_label_od as t_label_od,\n" +
                "       t1_t_question_content as t_question_content,\n" +
                "       t1_final_star_score as final_star_score,\n" +
                "       t1_template_code as template_code,\n" +
                "       t1_template_name as template_name,\n" +
                "       t1_question_type as question_type,\n" +
                "       t1_supplier_name as supplier_name,\n" +
                "       t1_t_request_evaluator_id as t_request_evaluator_id,\n" +
                "       t1_city_code as city_code,\n" +
                "       t2_city_general_manager_staff_name as city_general_manager_staff_name,\n" +
                "       t2_center_general_manager_staff_name as center_general_manager_staff_name,\n" +
                "       t2_staff_code as staff_code,\n" +
                "       t2_staff_name as staff_name,\n" +
                "    \n" +
                "       t2_deptid_name as deptid_name,\n" +
                "       t2_deptid_l1_name as deptid_l1_name,\n" +
                "       t2_deptid_l2_name as deptid_l2_name,\n" +
                "       t2_deptid_l3_name as deptid_l3_name,\n" +
                "       t2_deptid_l4_name as deptid_l4_name,\n" +
                "       t2_deptid_l5_name as deptid_l5_name,\n" +
                "       t2_deptid_l6_name as deptid_l6_name,\n" +
                "       t2_deptid_l7_name as deptid_l7_name,\n" +
                "       t2_job_position_desc as job_position_desc,\n" +
                "       t2_job_code_desc as job_code_desc,\n" +
                "       t2_job_grade_desc as job_grade_desc,\n" +
                "       t2_job_level as job_level,\n" +
                "       t2_is_resign as is_resign,\n" +
                "       t2_join_ziroom_date as join_ziroom_date,\n" +
                "       t2_manager_staff_name as manager_staff_name,\n" +
                "       t2_director_staff_name as director_staff_name,\n" +
                "       t3_room_id as room_id,\n" +
                "       t3_room_code as room_code,\n" +
                "       t3_room_name as room_name,\n" +
                "       t3_rent_contract_type_name as rent_contract_type_name,\n" +
                "       t3_service_keeper_code as service_keeper_code,\n" +
                "       t3_is_rent as is_rent,\n" +
                "       t3_is_first_rent as is_first_rent,\n" +
                "       t3_room_type_name as room_type_name,\n" +
                "       nvl(t3_product_line_name,t4_product_line_name) as product_line_name,\n" +
                "       t3_decorate_degree_name as decorate_degree_name,\n" +
                "       t3_rent_contract_code as rent_contract_code,\n" +
                "       t3_rent_sign_date as rent_sign_date,\n" +
                "       t3_rent_start_date as rent_start_date,\n" +
                "       nvl(t3_product_version_code,t4_product_version_code) as product_version_code,\n" +
                "       t3_resblock_id as resblock_id,\n" +
                "       t3_resblock_name as resblock_name,\n" +
                "       t3_after_reform_bedroom_amount as after_reform_bedroom_amount,\n" +
                "       t4_hire_contract_type_name as hire_contract_type_name,\n" +
                "       t4_accept_time as accept_time,\n" +
                "       t4_old_hire_contract_id as old_hire_contract_id,\n" +
                "       t4_hire_conf_staff_code as hire_conf_staff_code ,\n" +
                "       t4_hire_belong_conf_staff_code as hire_belong_conf_staff_code,\n" +
                "       t4_hire_contract_code as hire_contract_code,\n" +
                "       t4_house_belong_keeper_code as house_belong_keeper_code,                     \n" +
                "       t4_hire_start_date as hire_start_date,                                                                            \n" +
                "       t5_configuration_type as configuration_type,\n" +
                "\tt7_staff_name as staff_name_jl,\n" +
                "\tt7_city_name as city_name_jl,\n" +
                "\tt7_deptid_l3_name as deptid_l3_name_jl,\n" +
                "\tt7_deptid_l4_name as deptid_l4_name_jl,\n" +
                "\tt7_deptid_l5_name as deptid_l5_name_jl,\n" +
                "\tt8_staff_name as staff_name_zj,\n" +
                "\tt8_city_name as city_name_zj,\n" +
                "\tt8_deptid_l3_name as deptid_l3_name_zj,\n" +
                "\tt8_deptid_l4_name as deptid_l4_name_zj\n" +
                "\n" +
                "from\n" +
                "\n" +
                "  (SELECT \n" +
                "         \n" +
                "          t_collect_id as t1_t_collect_id,\n" +
                "          t_be_evaluator_type AS t1_t_be_evaluator_type,\n" +
                "\n" +
                "          t_be_evaluator_name AS t1_t_be_evaluator_name,\n" +
                "\n" +
                "          pz_order_code AS t1_pz_order_code,\n" +
                "\n" +
                "          t_score_flag AS t1_t_score_flag,\n" +
                "\n" +
                "          t_label AS t1_t_label,\n" +
                "\n" +
                "          object_code AS t1_object_code,\n" +
                "\n" +
                "          group_manager_code AS t1_group_manager_code,\n" +
                "\n" +
                "          group_manager_name AS t1_group_manager_name,\n" +
                "\n" +
                "          dept_manager_code AS t1_dept_manager_code,\n" +
                "\n" +
                "          dept_manager_name AS t1_dept_manager_name,\n" +
                "\n" +
                "          t_evaluate_date AS t1_t_evaluate_date,\n" +
                "\n" +
                "          order_code AS t1_order_code,\n" +
                "\n" +
                "          t_be_evaluator_id AS t1_t_be_evaluator_id,\n" +
                "\n" +
                "          t_question_type AS t1_t_question_type,\n" +
                "\n" +
                "          t_name AS t1_t_name,\n" +
                "\n" +
                "          t_question_value AS t1_t_question_value,\n" +
                "\n" +
                "          t_question_option_id AS t1_t_question_option_id,\n" +
                "\n" +
                "          t_label_od AS t1_t_label_od,\n" +
                "\n" +
                "          t_question_content AS t1_t_question_content,\n" +
                "\n" +
                "          final_star_score AS t1_final_star_score,\n" +
                "\n" +
                "          template_code AS t1_template_code,\n" +
                "\n" +
                "          template_name AS t1_template_name,\n" +
                "\n" +
                "          question_type AS t1_question_type,\n" +
                "\n" +
                "          supplier_name AS t1_supplier_name,\n" +
                "\n" +
                "\n" +
                "          t_request_evaluator_id AS t1_t_request_evaluator_id,\n" +
                "\n" +
                "          city_code AS t1_city_code\n" +
                "\n" +
                "   FROM dwd.dwd_quality_evaluate_order_detail_f_d\n" +
                "\n" +
                "   WHERE\n" +
                "\n" +
                "     dt = '$[yyyyMMdd-1]' \n" +
                " \n" +
                "     --from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)\n" +
                "\n" +
                "     AND (t_question_type = \"XSJGCP\" --业主新签-增益-产品评价\n" +
                "\n" +
                "          OR t_question_type = \"YZXQPJ3\" --业主新签评价3.0\n" +
                "\n" +
                "          OR t_question_type = \"ZRKXQ3\" --自如客新签评价3  ZRKXQ3\n" +
                "          OR t_question_type = \"ZODKCP2\" --自如客带看产品评价2.0\n" +
                "          OR template_code = \"TP2020112615460008\" --业主新签产品评价3.0\n" +
                "OR template_code =\"TP2020030212270010\" --自如客新签3-产品-整租4.0\n" +
                "OR template_code =\"TP2020022014190002\" --自如客新签3-产品-整租\n" +
                "OR template_code =\"TP2020030212400018\" --自如客新签3-产品-友家\n" +
                "OR template_code =\"TP2020101515470014\" --业主新签-增益-产品评价\n" +
                "\n" +
                "        )\n" +
                "\n" +
                "     AND to_date(t_evaluate_date) >= \"2019-12-28\"\n" +
                "\n" +
                "  )  t1 --评价表\n" +
                "\n" +
                "LEFT JOIN\n" +
                "\n" +
                "  (SELECT center_general_manager_staff_name AS t2_center_general_manager_staff_name,\n" +
                "\n" +
                "          city_general_manager_staff_name AS t2_city_general_manager_staff_name,\n" +
                "\n" +
                "          staff_code AS t2_staff_code,\n" +
                "\n" +
                "          staff_name AS t2_staff_name,\n" +
                "\n" +
                "          \n" +
                "\n" +
                "          deptid_name AS t2_deptid_name,\n" +
                "\n" +
                "          deptid_l1_name AS t2_deptid_l1_name,\n" +
                "\n" +
                "          deptid_l2_name AS t2_deptid_l2_name,\n" +
                "\n" +
                "          deptid_l3_name AS t2_deptid_l3_name,\n" +
                "\n" +
                "          deptid_l4_name AS t2_deptid_l4_name,\n" +
                "\n" +
                "          deptid_l5_name AS t2_deptid_l5_name,\n" +
                "\n" +
                "          deptid_l6_name AS t2_deptid_l6_name,\n" +
                "\n" +
                "          deptid_l7_name AS t2_deptid_l7_name,\n" +
                "\n" +
                "          job_position_desc AS t2_job_position_desc,\n" +
                "\n" +
                "          job_code_desc AS t2_job_code_desc,\n" +
                "\n" +
                "          job_grade_desc AS t2_job_grade_desc,\n" +
                "\n" +
                "          job_level AS t2_job_level,\n" +
                "\n" +
                "          is_resign AS t2_is_resign,\n" +
                "\n" +
                "          join_ziroom_date AS t2_join_ziroom_date,\n" +
                "\n" +
                "          manager_staff_name AS t2_manager_staff_name,\n" +
                "\n" +
                "          director_staff_name AS t2_director_staff_name\n" +
                "\n" +
                "   FROM dim.dim_hr_staff_info_f_d\n" +
                "\n" +
                "   WHERE\n" +
                "\n" +
                "     dt = '$[yyyyMMdd-1]'\n" +
                "\n" +
                "     --from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)\n" +
                "\n" +
                "     AND is_resign = '0'\n" +
                "\n" +
                "     AND is_main_job = '1'\n" +
                "\n" +
                "  )  t2 --被评价人人员信息表\n" +
                "\n" +
                "ON t1.t1_t_be_evaluator_id = t2.t2_staff_code\n" +
                "\n" +
                "\n" +
                "\n" +
                "LEFT JOIN\n" +
                "\n" +
                "  (SELECT room_id AS t3_room_id,\n" +
                "\n" +
                "          room_code AS t3_room_code,\n" +
                "\n" +
                "          room_name AS t3_room_name,\n" +
                "\n" +
                "          rent_contract_type_name AS t3_rent_contract_type_name,\n" +
                "\n" +
                "          service_keeper_code AS t3_service_keeper_code,\n" +
                "\n" +
                "          is_rent AS t3_is_rent,\n" +
                "\n" +
                "          is_first_rent AS t3_is_first_rent,\n" +
                "\n" +
                "          room_type_name AS t3_room_type_name,\n" +
                "\n" +
                "          product_line_name AS t3_product_line_name,\n" +
                "\n" +
                "          decorate_degree_name AS t3_decorate_degree_name,\n" +
                "\n" +
                "          rent_contract_code AS t3_rent_contract_code,\n" +
                "\n" +
                "          rent_sign_date AS t3_rent_sign_date,\n" +
                "\n" +
                "          rent_start_date AS t3_rent_start_date,\n" +
                "\n" +
                "          product_version_code AS t3_product_version_code,\n" +
                "\n" +
                "          resblock_id AS t3_resblock_id,\n" +
                "\n" +
                "          resblock_name AS t3_resblock_name,\n" +
                "\n" +
                "          after_reform_bedroom_amount AS t3_after_reform_bedroom_amount\n" +
                "\n" +
                "   FROM dwd.dwd_rent_contract_detail_f_d\n" +
                "\n" +
                "   WHERE\n" +
                "   dt = '$[yyyyMMdd-1]'\n" +
                "    --from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)\n" +
                "\n" +
                "  )  t3 --出房合同信息\n" +
                "\n" +
                " ON t1.t1_order_code = t3.t3_rent_contract_code\n" +
                "\n" +
                "\n" +
                "\n" +
                "LEFT JOIN\n" +
                "\n" +
                "  (SELECT\n" +
                "\n" +
                "          hire_contract_type_name AS t4_hire_contract_type_name,\n" +
                "\n" +
                "          accept_time AS t4_accept_time,\n" +
                "\n" +
                "          old_hire_contract_id AS t4_old_hire_contract_id,\n" +
                "\n" +
                "          hire_conf_staff_code AS t4_hire_conf_staff_code,\n" +
                "\n" +
                "          hire_belong_conf_staff_code AS t4_hire_belong_conf_staff_code,\n" +
                "\n" +
                "          hire_contract_code AS t4_hire_contract_code,\n" +
                "\n" +
                "          product_version_code AS t4_product_version_code,\n" +
                "\n" +
                "          resblock_id AS t4_resblock_id,\n" +
                "\n" +
                "          resblock_name AS t4_resblock_name,\n" +
                "\n" +
                "          house_belong_keeper_code AS t4_house_belong_keeper_code,\n" +
                "\n" +
                "          hire_start_date AS t4_hire_start_date,\n" +
                "\n" +
                "          after_reform_bedroom_amount AS t4_after_reform_bedroom_amount,\n" +
                "\n" +
                "product_line_name as t4_product_line_name\n" +
                "\n" +
                "   FROM dwd.dwd_hire_contract_detail_f_d\n" +
                "\n" +
                "   WHERE dt = '$[yyyyMMdd-1]'\n" +
                "   --from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)\n" +
                "\n" +
                "   ) t4 --收房合同信息\n" +
                "\n" +
                " ON t1.t1_order_code = t4.t4_hire_contract_code\n" +
                "\n" +
                "\n" +
                "\n" +
                "LEFT JOIN\n" +
                "\n" +
                "  (SELECT\n" +
                "    configuration_type AS t5_configuration_type,\n" +
                "    order_code AS t5_order_code\n" +
                "   FROM dwd.dwd_config_order_detail_f_d\n" +
                "\n" +
                "   WHERE dt = '$[yyyyMMdd-1]'\n" +
                "   and configuration_type in ('0','4','5','7')\n" +
                "   --from_unixtime(unix_timestamp(dt,'yyyymmdd'),'yyyy-mm-dd')=date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1)\n" +
                "\n" +
                "  )  t5 --配置订单\n" +
                "\n" +
                " ON t1.t1_pz_order_code = t5.t5_order_code\n" +
                " \n" +
                " left join \n" +
                " \t(select staff_code as t7_staff_code \n" +
                " \t\t\t,staff_name as t7_staff_name\n" +
                " \t\t\t,city_name as t7_city_name\n" +
                " \t\t\t,deptid_l3_name as t7_deptid_l3_name\n" +
                " \t\t\t,deptid_l4_name as t7_deptid_l4_name\n" +
                " \t\t\t,deptid_l5_name as t7_deptid_l5_name\n" +
                "     from dim.dim_hr_staff_info_f_d\n" +
                "     where dt = '$[yyyyMMdd-1]' and is_resign = '0' and is_main_job = '1') t7 --被评价经理人员信息\n" +
                "  on t1.t1_group_manager_code = t7.t7_staff_code\n" +
                "\n" +
                "left join \n" +
                "\t(select staff_code as t8_staff_code\n" +
                "\t\t,staff_name as t8_staff_name\n" +
                "\t\t,city_name as t8_city_name\n" +
                "\t\t,deptid_l3_name as t8_deptid_l3_name\n" +
                "\t\t,deptid_l4_name as t8_deptid_l4_name\n" +
                "     from dim.dim_hr_staff_info_f_d\n" +
                "     where dt = '$[yyyyMMdd-1]'\n" +
                "               and is_resign = '0'\n" +
                "               and is_main_job = '1') t8 --被评价总监人员信息\n" +
                "     on t1.t1_dept_manager_code = t8.t8_staff_code");
        System.out.println(JSON.toJSONString(json, SerializerFeature.WriteMapNullValue));
    }
}
