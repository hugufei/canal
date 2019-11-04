package com.alibaba.otter.canal.common.zookeeper.running;

import java.util.Map;

/**
 * {@linkplain ServerRunningMonitor}管理容器，使用static进行数据全局共享
 *
 * @author jianghang 2012-12-3 下午09:32:06
 * @version 1.0.0
 */
public class ServerRunningMonitors {

    private static ServerRunningData serverData;
    private static Map runningMonitors; // <String,ServerRunningMonitor>

    public static ServerRunningData getServerData() {
        return serverData;
    }

    public static Map<String, ServerRunningMonitor> getRunningMonitors() {
        return runningMonitors;
    }

    public static ServerRunningMonitor getRunningMonitor(String destination) {
        return (ServerRunningMonitor) runningMonitors.get(destination);
    }

    public static void setServerData(ServerRunningData serverData) {
        ServerRunningMonitors.serverData = serverData;
    }

    // 其中Map的key是destination，value是ServerRunningMonitor，
    // 也就是说针对每一个destination都有一个ServerRunningMonitor来监控。
    public static void setRunningMonitors(Map<String, ServerRunningMonitor> runningMonitors) {
        ServerRunningMonitors.runningMonitors = runningMonitors;
    }

}
