package com.xxl.job.core.biz.model;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by xuxueli on 16/7/22.
 */
@Data
public class TriggerParam implements Serializable{
    private static final long serialVersionUID = 42L;

    private int jobId;

    // 执行器信息
    private String executorHandler;
    private String executorParams;
    private String executorBlockStrategy;
    private int executorTimeout;

    private long logId;
    private long logDateTime;

    // 运行模式信息
    private String glueType;
    private String glueSource;
    private long glueUpdatetime;

    // 广播分片信息
    private int broadcastIndex;
    private int broadcastTotal;

    @Override
    public String toString() {
        return "TriggerParam{" +
                "jobId=" + jobId +
                ", executorHandler='" + executorHandler + '\'' +
                ", executorParams='" + executorParams + '\'' +
                ", executorBlockStrategy='" + executorBlockStrategy + '\'' +
                ", executorTimeout=" + executorTimeout +
                ", logId=" + logId +
                ", logDateTime=" + logDateTime +
                ", glueType='" + glueType + '\'' +
                ", glueSource='" + glueSource + '\'' +
                ", glueUpdatetime=" + glueUpdatetime +
                ", broadcastIndex=" + broadcastIndex +
                ", broadcastTotal=" + broadcastTotal +
                '}';
    }

}
