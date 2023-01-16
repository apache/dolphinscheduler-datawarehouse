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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.datawarehouse.be.enums.ProcessCronEnum;
import com.datawarehouse.be.exceptions.ProcessParamsException;
import com.datawarehouse.be.pojo.dos.CustomWarningDO;
import com.datawarehouse.be.pojo.dos.DepDO;
import com.datawarehouse.be.pojo.dto.QueryDto;
import com.datawarehouse.be.pojo.vo.SqlParsingVO;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Api(tags = "AddEditProcessParam")
public class AddEditProcessParam {

    private static final Logger logger = LoggerFactory.getLogger(AddEditProcessParam.class);

    /**
     * sql 拼接请求参数
     */
    @ApiModelProperty(name = "queryDto", value = "sql拼接请求参数", dataType = "com.datawarehouse.be.pojo.dto.QueryDto")
    private QueryDto queryDto;

    /**
     * 告警参数：用于建立告警
     */
    @ApiModelProperty(name = "customWarningParam", value = "告警参数", dataType = "com.datawarehouse.be.pojo.param.CustomWarningParam")
    private CustomWarningDO customWarningDO;

    /**
     * process Id
     */
    @ApiModelProperty(name = "processId", value = "工作流Id", dataType = "java.lang.Integer")
    private Long processId;

    /**
     * process Id
     */
    @ApiModelProperty(name = "Sql", value = "工作流SQL", dataType = "java.lang.String", example = "INSERT OVERWRITE TABLE test.opentsdb partition(dt='$[yyyyMMdd-1]')\n"
        +
            "select t.rent_contract_code from dwd.dwd_rent_contract_detail_f_d t where t.dt = '$[yyyyMMdd-1]';")
    private String sql;

    /**
     * name
     */
    @ApiModelProperty(name = "name", value = "工作流名称", dataType = "java.lang.String", example = "test", required = true)
    @JsonProperty("name")
    private String name;
    /**
     * description
     */
    @ApiModelProperty(name = "name", value = "工作流描述", dataType = "java.lang.String", example = "测试工作流", required = true)
    @JsonProperty("description")
    private String description;

    /**
     * 周期
     */
    @ApiModelProperty(name = "cycle", value = "工作流周期", dataType = "java.lang.String", example = "每日")
    private String cycle;
    /**
     * dateValue
     */
    @ApiModelProperty(name = "dateValue", value = "工作流周期值", dataType = "java.lang.String")
    private String dateValue;

    /**
     * timeValue
     */
    @ApiModelProperty(name = "timeValue", value = "工作流周期时间值", dataType = "java.lang.String")
    private String timeValue;

    /**
     * taskCount
     */
    @ApiModelProperty(name = "taskCount", value = "taskCount", dataType = "java.lang.Integer", hidden = true)
    private Integer taskCount;
    /**
     * startDate
     */
    @ApiModelProperty(name = "startDate", value = "工作流开始时间", dataType = "java.lang.String", hidden = true)
    private String startDate;
    /**
     * retries
     */
    @ApiModelProperty(name = "retries", value = "工作流重试次数", dataType = "java.lang.Integer", hidden = true)
    private Integer retries;
    /**
     * pool
     */
    @ApiModelProperty(name = "pool", value = "pool", dataType = "java.lang.String", hidden = true)
    private String pool;

    /**
     * 工作流依赖集合
     */
    @ApiModelProperty(name = "depDOList", value = "工作流依赖集合", dataType = "java.lang.List<DepDO>", example = "")
    private List<DepDO> depDOList;

    /**
     * 工作流的产出（表）
     */
    @ApiModelProperty(name = "outPutDO", value = "工作流产出集合", dataType = "java.lang.List<ExportDO>", example = "")
    private SqlParsingVO.OutPutDO outPutDO;

    /**
     * 工作流中导出
     */
    @ApiModelProperty(name = "exportDO", value = "工作流产出集合", dataType = "java.lang.List<ExportDO>", example = "")
    private SqlParsingVO.ExportDO exportDO;

    @ApiModelProperty(name = "projectCode", value = "项目Id", dataType = "java.lang.Integer", example = "1")
    private long projectId;

    @ApiModelProperty(name = "projectName", value = "项目名称", dataType = "java.lang.String", example = "test")
    private String projectName;

    public void init() {
        if (taskCount == null) {
            taskCount = 3;
        }
        if (StringUtils.isBlank(startDate)) {
            startDate = new DateTime().toString("yyyy-MM-dd HH:mm:ss");
        }

        if (retries == null) {
            this.retries = 3;
        }
    }

    public static void main(String[] args) {
        System.out.println(NumberUtils.isDigits("00"));
        System.out.println(NumberUtils.isDigits("01"));
        System.out.println(NumberUtils.isDigits("10"));
    }

    public void verify() {

        if (StringUtils.isBlank(this.sql)) {
            throw new ProcessParamsException("工作流SQL不能为空，请填写后再试~");
        }

        if (StringUtils.isBlank(this.name)) {
            throw new ProcessParamsException("工作流名称不能为空，请填写后再试~");
        }
        if (StringUtils.isBlank(this.cycle)) {
            throw new ProcessParamsException("工作流周期不能为空，请填写后再试~");
        } else {
            if (!Arrays.stream(ProcessCronEnum.values()).anyMatch(e -> this.cycle.equals(e.getCycle()))) {
                throw new ProcessParamsException("工作流周期格式不符合要求，请填写后再试~");
            }
        }
        if (StringUtils.isNotBlank(this.dateValue)) {
            if (!ProcessCronEnum.isExistsOfDateValue(this.cycle, this.dateValue)) {
                throw new ProcessParamsException("工作流周期值格式不符合要求，请填写后再试~");
            }
        }
        if (CollectionUtils.isEmpty(depDOList)) {
            throw new ProcessParamsException("工作流中依赖任务不能为空，请确认~");
        } else {
            depDOList.forEach(e -> e.verify());
        }
        if (outPutDO == null) {
            throw new ProcessParamsException("工作流中数据产出不能为空，请确认~");
        } else {
            outPutDO.verify();
        }
        if (StringUtils.isBlank(this.timeValue)) {
            throw new ProcessParamsException("工作流中周期对应时间不能为空，请确认~");
        } else {
            String [] timeArray = this.timeValue.split(":");
            if (timeArray.length != 2) {
                throw new ProcessParamsException("工作流中周期对应时间格式错误，正确格式为->小时:分钟，例如->08:00，请确认~");
            }
            String house = timeArray[0];
            String min = timeArray[1];

            if (StringUtils.isBlank(house) || !NumberUtils.isDigits(house)) {
                throw new ProcessParamsException("工作流中周期对应时间格式错误，小时不能为空且必须为数字，请确认~");
            }
            if (StringUtils.isBlank(min) || !NumberUtils.isDigits(min)) {
                throw new ProcessParamsException("工作流中周期对应时间格式错误，分钟不能为空且必须为数字，请确认~");
            }
        }
        if (this.exportDO != null) {
            boolean whetherNotNullDatasource = Optional.ofNullable(this.exportDO).map(e -> StringUtils.isNotBlank(e.getDatasourceName())).orElse(false);
            // 数据源不为空，判断附加内容
            if (whetherNotNullDatasource) {
                String datasourceJson = Optional.ofNullable(this.exportDO).map(e -> e.getDatasourceJson()).orElse(null);
                if (datasourceJson == null) {
                    throw new ProcessParamsException("工作流中导出的，导出的JSON入参(datasourceJson)不能为空，请确认~");
                }
                SqlParsingVO.ExportDO.ClickhouseDO clickhouseDO;
                try {
                    clickhouseDO = JSON.parseObject(datasourceJson, SqlParsingVO.ExportDO.ClickhouseDO.class);
                } catch (Exception e) {
                    throw new ProcessParamsException(String.format("工作流中导出的，导出的JSON入参datasourceJson：%s格式不对，请确认~", datasourceJson));
                }

                Integer retentionDays = clickhouseDO.getRetentionDays();
                if (retentionDays == null) {
                    throw new ProcessParamsException(String.format("工作流中导出的，retentionDays字段不能为空，datasourceJson：%s，请确认~", datasourceJson));
                }
            }
        }
    }
}
