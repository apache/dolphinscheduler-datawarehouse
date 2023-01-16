set names utf8mb4;
set foreign_key_checks = 0;

CREATE TABLE `t_ds_process_definition_io` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `process_definition_id` int(11) DEFAULT NULL,
      `process_definition_version` bigint(11) DEFAULT NULL,
      `io_type` int(11) DEFAULT NULL COMMENT 'input/output',
      `io_name` varchar(255) DEFAULT NULL,
      `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      `task_id` varchar(100) DEFAULT NULL COMMENT '任务主键id',
      `task_name` varchar(255) DEFAULT NULL COMMENT '任务名称',
      `process_definition_name` varchar(255) DEFAULT NULL COMMENT '工作流名称',
      PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=10332 DEFAULT CHARSET=utf8mb4;

CREATE TABLE `t_dw_process_type` (
      `id` int(11) NOT NULL AUTO_INCREMENT COMMENT 'Uniquely identified column',
      `process_id` int(11) NOT NULL COMMENT 'process id',
      `process_type` tinyint(4) DEFAULT NULL COMMENT 'process type',
      `process_data_structure` longtext COMMENT 'Used for visual echo',
      PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3548 DEFAULT CHARSET=utf8;

