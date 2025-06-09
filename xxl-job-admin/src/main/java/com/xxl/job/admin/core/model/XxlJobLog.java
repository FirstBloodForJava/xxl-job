package com.xxl.job.admin.core.model;

import lombok.Data;

import java.util.Date;

/**
 * xxl-job log, used to track trigger process
 * @author xuxueli  2015-12-19 23:19:09
 * xxl_job_log
 */
@Data
public class XxlJobLog {
	
	private long id;

	// 任务信息
	private int jobGroup;
	private int jobId;

	// 执行器信息
	private String executorAddress;
	private String executorHandler;
	private String executorParam;
	private String executorShardingParam;
	private int executorFailRetryCount;
	
	// 触发器信息
	private Date triggerTime;
	private int triggerCode;
	private String triggerMsg;
	
	// 执行信息
	private Date handleTime;
	private int handleCode; // 任务执行状态
	private String handleMsg;//

	// 告警状态 0-默认、1-无需告警、2-告警成功、3-告警失败
	private int alarmStatus;

}
