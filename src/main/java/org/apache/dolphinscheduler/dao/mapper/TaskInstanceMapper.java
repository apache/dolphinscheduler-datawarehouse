package org.apache.dolphinscheduler.dao.mapper;

import org.apache.dolphinscheduler.common.enums.Flag;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.ibatis.annotations.Param;

import java.util.List;

public interface TaskInstanceMapper {

    List<TaskInstance> queryNewestTaskInstanceByProcessId(@Param("processInstanceId") int processInstanceId, @Param("flag") Flag flag);

}
