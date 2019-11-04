package com.alibaba.otter.canal.instance.core;

/**
 * @author zebin.xuzb @ 2012-7-12
 * @version 1.0.0
 */

// 用于创建CanalInstance实例
// 作用就是为canal.properties文件中canal.destinations配置项, 列出的每个destination，创建一个CanalInstance实例
public interface CanalInstanceGenerator {

    /**
     * 通过 destination 产生特定的 {@link CanalInstance}
     * 
     * @param destination
     * @return
     */
    CanalInstance generate(String destination);
}
