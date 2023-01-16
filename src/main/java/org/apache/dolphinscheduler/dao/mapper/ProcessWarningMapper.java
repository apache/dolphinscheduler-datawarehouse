package org.apache.dolphinscheduler.dao.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.dolphinscheduler.dao.entity.ProcessWarning;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @AUTHOR :  wanglj9
 * @DATE :  2021/12/13 16:40
 */
// TODO 实现批量插入
public interface ProcessWarningMapper extends BaseMapper<ProcessWarning> {

    /**
     * 批量插入
     * @param processWarnings
     * @return
     */
    int batchInsert(List<ProcessWarning> processWarnings);


    /**
     * 批量查询
     * @param ProcessId
     * @param type
     * @return
     */
    List<Integer> selectByProcessIdAndWarningType(@Param("process_id") Long ProcessId, @Param("type") int type);


    /**
     * 批量删除
     * @param ProcessId
     */
    void deleteProcessWarningByProcessId(@Param("process_id") Long ProcessId);

}
