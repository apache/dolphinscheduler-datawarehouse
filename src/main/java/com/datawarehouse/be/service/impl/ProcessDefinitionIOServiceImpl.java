package com.datawarehouse.be.service.impl;

import com.datawarehouse.be.pojo.dos.ProcessDefinitionIODO;
import com.datawarehouse.be.service.ProcessDefinitionIOService;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.api.enums.Status;
import org.apache.dolphinscheduler.api.service.impl.BaseServiceImpl;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.enums.IOType;
import org.apache.dolphinscheduler.dao.entity.ProcessDefinitionIO;
import org.apache.dolphinscheduler.dao.mapper.ProcessDefinitionIOMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ProcessDefinitionIOServiceImpl extends BaseServiceImpl implements ProcessDefinitionIOService {

    @Autowired
    private ProcessDefinitionIOMapper processDefinitionIOMapper;


    @Override
    public Map<String,Object> queryByIO(IOType ioType, List<String> outputs) {
        Map<String, Object> result = new HashMap<>();
        List<ProcessDefinitionIO> processDefinitionIOS = processDefinitionIOMapper.queryByIOTypeAndIONames(ioType.getCode(), outputs);
        result.put(Constants.DATA_LIST, processDefinitionIOS);
        putMsg(result, Status.SUCCESS);
        return result;
    }

    @Override
    public List<ProcessDefinitionIODO> query(IOType ioType, List<String> tableList) {
        if (ioType == null || CollectionUtils.isEmpty(tableList)) {
            return null;
        }
        List<ProcessDefinitionIO> list = processDefinitionIOMapper.queryByIOTypeAndIONames(ioType.getCode(), tableList);
        if (CollectionUtils.isEmpty(list)) {
            return null;
        }
        return list.stream().map(e -> new ProcessDefinitionIODO(e.getProcessDefinitionCode(), e.getProcessDefinitionName(), e.getTaskId(), e.getTaskName(), e.getIoName())).collect(Collectors.toList());
    }
}
