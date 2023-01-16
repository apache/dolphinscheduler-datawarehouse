package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import org.apache.dolphinscheduler.dao.entity.OutPutExportEntity;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.LinkedList;

@Repository
public interface ExportDataMapper extends BaseMapper<ProcessInstance> {
    IPage<ProcessInstance> queryExportDataProcessInstanceListPaging(Page<ProcessInstance> page,
                                                                    @Param("processDefinitionId") Long processDefinitionId,
                                                                    @Param("startTimeFilterVal")String startTimeFilterVal);

    LinkedList<OutPutExportEntity> queryPartitionDays(@Param("processDefinitionId") Long processDefinitionId, @Param("dataSourceName")String dataSourceName);

    Integer checkWhetherExport(@Param("processId") Long processId);

    LinkedList<String> queryProcessEndTime(@Param("processDefinitionId") Long processDefinitionId);

}
