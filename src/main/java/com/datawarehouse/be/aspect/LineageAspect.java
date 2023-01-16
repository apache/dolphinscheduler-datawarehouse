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

package com.datawarehouse.be.aspect;

import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.ProcessDefinitionIOService;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.dao.entity.DagData;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinition;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionMapper;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

@Aspect
@Component
public class LineageAspect {
    private static final Logger logger = LoggerFactory.getLogger(LineageAspect.class);
    @Autowired
    private ProcessDefinitionIOService processDefinitionIOService;

    @Autowired
    private ProcessDefinitionMapper processDefinitionMapper;


    @Around("execution(* org.apache.dolphinscheduler.api.service.ProcessDefinitionService.createProcessDefinition(..))")
    public Object createLineage(ProceedingJoinPoint joinPoint) throws Throwable {
         return buildLineage(joinPoint);
    }

    @Around("execution(* org.apache.dolphinscheduler.api.service.ProcessDefinitionService.updateProcessDefinition(..))")
    public Object updateLineage(ProceedingJoinPoint joinPoint) throws Throwable {
        return buildLineage(joinPoint);
    }

    @Around("execution(* org.apache.dolphinscheduler.api.service.ProcessDefinitionService.batchCopyProcessDefinition(..))")
    public Object copyLineage(ProceedingJoinPoint joinPoint) throws Throwable {
        return buildLineage(joinPoint);
    }

   private Object buildLineage(ProceedingJoinPoint joinPoint) throws Throwable{
       //获取方法参数值数组
       Object[] args = joinPoint.getArgs();
       logger.info("请求参数为", args);

       Map<String, Object> result = (Map)joinPoint.proceed(args);
       logger.info("响应结果为{}",result);

       boolean isSuccess = Optional.ofNullable(result).map(e -> Status.SUCCESS.equals(e.get(Constants.STATUS))).orElse(false);
       if (isSuccess) {
           ProcessDefinition processDefinition = (ProcessDefinition) result.get("data");
           Long processDefinitionCode = processDefinition.getCode();
           ProcessDefinition processDefinition1 = processDefinitionMapper.queryByCode(processDefinitionCode);
           // 新增、修改工作流
           processDefinitionIOService.saveProcessDefinitionIO(processDefinition1);
       }
       return result;
    }

    @Around("execution(* org.apache.dolphinscheduler.api.service.ProcessDefinitionService.queryProcessDefinitionByCode(..))")
    public Object deleteLineage(ProceedingJoinPoint joinPoint) throws Throwable {

        //获取方法参数值数组
        Object[] args = joinPoint.getArgs();
        logger.info("请求参数为", args);

        Map<String, Object> result = (Map)joinPoint.proceed(args);
        logger.info("响应结果为{}",result);
        //如果这里不返回result，则目标对象实际返回值会被置为null

        // 工作流处理成功后，进行血缘操作，失败则忽略血缘操作
        boolean isSuccess = Optional.ofNullable(result).map(e -> Status.SUCCESS.equals(e.get(Constants.STATUS))).orElse(false);
        if (isSuccess) {
            DagData dagData = (DagData)  result.get("data");
            long processDefinitionCode = dagData.getProcessDefinition().getCode();
            // 删除工作流
            processDefinitionIOService.deleteProcessDefinitionIO(processDefinitionCode);

        }
        return result;
    }
}