package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.dolphinscheduler.dao.entity.ProcessCreateType;
import org.apache.ibatis.annotations.Param;

/**
 * @Author luanzw
 * @Date 2021/12/17
 */
public interface ProcessCreateTypeMapper extends BaseMapper<ProcessCreateType> {

    /**
     * 插入一条条目
     */
    void insertItem(ProcessCreateType processCreateTypeItem);

    /**
     * 根据工作流id删除一条条目
     */
    void deleteItemByProcessID(@Param("process_id") long processId);

    /**
     * 根据工作流id查询一条条目
     */
    ProcessCreateType selectItemByProcessID(@Param("process_id") Long processID);

}
