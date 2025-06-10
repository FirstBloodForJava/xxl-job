package com.xxl.job.executor;

import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.executor.service.jobhandler.ExecJobHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author xuxueli 2018-10-28 00:38:13
 */
@SpringBootApplication
public class XxlJobExecutorApplication {

	public static void main(String[] args) {
        SpringApplication.run(XxlJobExecutorApplication.class, args);
		XxlJobExecutor.registJobHandler(ExecJobHandler.class.getSimpleName(), new ExecJobHandler());
	}

}