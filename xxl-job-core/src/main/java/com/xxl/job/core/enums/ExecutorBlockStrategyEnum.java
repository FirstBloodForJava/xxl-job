package com.xxl.job.core.enums;

/**
 * Created by xuxueli on 17/5/9.
 * 阻塞处理策略
 */
public enum ExecutorBlockStrategyEnum {

    /**
     * 单机串行
     */
    SERIAL_EXECUTION("Serial execution"),
    /*CONCURRENT_EXECUTION("并行"),*/
    /**
     * 直接丢弃
     */
    DISCARD_LATER("Discard"),
    /**
     * 覆盖之前调度
     */
    COVER_EARLY("Cover Early");

    private String title;

    private ExecutorBlockStrategyEnum(String title) {
        this.title = title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    /**
     * @param name        阻塞策略名称
     * @param defaultItem 不匹配的默认策略
     * @return
     */
    public static ExecutorBlockStrategyEnum match(String name, ExecutorBlockStrategyEnum defaultItem) {
        if (name != null) {
            for (ExecutorBlockStrategyEnum item : ExecutorBlockStrategyEnum.values()) {
                if (item.name().equals(name)) {
                    return item;
                }
            }
        }
        return defaultItem;
    }
}
