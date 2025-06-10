package com.xxl.job.executor.service.jobhandler;

import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.IJobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author ouyangcm
 * create 2025/6/10 14:42
 * Bean 类模式 Job执行器
 */
@SuppressWarnings(value = "all")
public class ExecJobHandler extends IJobHandler {

    private static final Logger logger = LoggerFactory.getLogger(ExecJobHandler.class);

    @Override
    public void execute() throws Exception {
        String command = XxlJobHelper.getJobParam();

        if (!StringUtils.hasText(command)) {
            XxlJobHelper.handleFail("command param is empty: " + command);
            return;
        }
        int exitValue = -1;

        BufferedReader bufferedReader = null;
        try {

            // 下面命令可用直接传命令，例如 ping 127.0.0.1
            Process process = Runtime.getRuntime().exec(command);

            BufferedInputStream bufferedInputStream = new BufferedInputStream(process.getInputStream());
            bufferedReader = new BufferedReader(new InputStreamReader(bufferedInputStream, System.getProperty("os.name").contains("Windows") ? "GBK" : System.getProperty("file.encoding")));

            // 记录命令执行结果
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                XxlJobHelper.log(line);
            }

            // 等待命令结束
            process.waitFor();
            exitValue = process.exitValue();
        } catch (Exception e) {
            XxlJobHelper.log(e);
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }

        if (exitValue == 0) {

        } else {
            XxlJobHelper.handleFail("command exit value(" + exitValue + ") is failed");
        }
    }


    public void init() throws Exception {
        logger.info(ExecJobHandler.class.getSimpleName() + " init");
    }

    public void destroy() throws Exception {
        logger.info(ExecJobHandler.class.getSimpleName() + " destroy");
    }
}
