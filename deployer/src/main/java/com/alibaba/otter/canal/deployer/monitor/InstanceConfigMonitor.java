package com.alibaba.otter.canal.deployer.monitor;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * 监听instance file的文件变化，触发instance start/stop等操作
 * 
 * @author jianghang 2013-2-6 下午06:19:56
 * @version 1.0.1
 */
public interface InstanceConfigMonitor extends CanalLifeCycle {

    // 当需要对一个destination进行监听时，调用register方法
    void register(String destination, InstanceAction action);

    // 当取消对一个destination监听时，调用unregister方法。
    // 事实上，unregister方法在canal 内部并没有有任何地方被调用，也就是说，某个destination如果开启了autoScan=true，那么你是无法在运行时停止对其进行监控的。如果要停止，你可以选择将对应的目录删除。
    void unregister(String destination);

}
