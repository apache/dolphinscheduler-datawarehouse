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

package com.datawarehouse.be.dao.hive;

import com.datawarehouse.be.pojo.vo.StaffInfoVO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.LinkedList;

@Mapper
public interface HiveTwoStaffMapper {

    @Select("SELECT\n" +
            "    staff_name as name,\n" +
            "    split(email_address, '@')[0] as userName\n" +
            "FROM\n" +
            "    (\n" +
            "        SELECT\n" +
            "            staff_code,\n" +
            "            staff_name,\n" +
            "            email_address,\n" +
            "            job_code,\n" +
            "            city_code,\n" +
            "            deptid_l2_plus,\n" +
            "            deptid_l3,\n" +
            "            deptid_l4,\n" +
            "            deptid_l5,\n" +
            "            city_name,\n" +
            "            deptid_l3_name,\n" +
            "            deptid_l4_name,\n" +
            "            deptid_l5_name,\n" +
            "            deptid,\n" +
            "            deptid_name,\n" +
            "            phone,\n" +
            "            row_number() over (\n" +
            "                partition BY staff_code\n" +
            "                ORDER BY\n" +
            "                    city_code\n" +
            "            ) num\n" +
            "        FROM\n" +
            "            dwd.dwd_hr_staff_latest_info_real_leader_f_d \n" +
            "        WHERE\n" +
            "            dt = from_unixtime(unix_timestamp(date_add(cast(current_date() as string), -1),'yyyy-MM-dd'),'yyyyMMdd')\n" +
            "            AND is_resign = '0' --在职\n" +
            "            AND is_main_job = '1' --主职\n" +
            "            AND email_address like CONCAT(#{keyword}, '%')" +
            "    ) t\n" +
            "WHERE\n" +
            "    num = 1\n" +
            "limit 20")
    LinkedList<StaffInfoVO> getStaffInfo(@Param("keyword") String keyword);
}
