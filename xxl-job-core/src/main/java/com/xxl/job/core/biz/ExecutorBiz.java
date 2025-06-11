package com.xxl.job.core.biz;

import com.xxl.job.core.biz.model.*;

/**
 * Created by xuxueli on 17/3/1.
 */
public interface ExecutorBiz {

    /**
     * 心跳检测 调度中心调度/执行器执行
     * @return
     */
    public ReturnT<String> beat();

    /**
     * 任务检测 调度中心调度/执行器执行
     *
     * @param idleBeatParam
     * @return
     */
    public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam);

    /**
     * run 任务执行 调度中心调度/执行器执行
     * @param triggerParam
     * @return
     */
    public ReturnT<String> run(TriggerParam triggerParam);

    /**
     * kill 关闭任务 调度中心调度/执行器执行
     * @param killParam
     * @return
     */
    public ReturnT<String> kill(KillParam killParam);

    /**
     * 读取任务日志 调度中心调度/执行器执行
     * @param logParam
     * @return
     */
    public ReturnT<LogResult> log(LogParam logParam);

}
