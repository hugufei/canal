package com.alibaba.otter.canal.deployer;

import java.util.Map;
import java.util.Properties;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.common.zookeeper.ZookeeperPathUtils;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningData;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningListener;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitor;
import com.alibaba.otter.canal.common.zookeeper.running.ServerRunningMonitors;
import com.alibaba.otter.canal.deployer.InstanceConfig.InstanceMode;
import com.alibaba.otter.canal.deployer.monitor.InstanceAction;
import com.alibaba.otter.canal.deployer.monitor.InstanceConfigMonitor;
import com.alibaba.otter.canal.deployer.monitor.ManagerInstanceConfigMonitor;
import com.alibaba.otter.canal.deployer.monitor.SpringInstanceConfigMonitor;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.instance.manager.CanalConfigClient;
import com.alibaba.otter.canal.instance.manager.ManagerCanalInstanceGenerator;
import com.alibaba.otter.canal.instance.spring.SpringCanalInstanceGenerator;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.exception.CanalServerException;
import com.alibaba.otter.canal.server.netty.CanalServerWithNetty;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.collect.MigrateMap;

/**
 * canal调度控制器
 *
 * @author jianghang 2012-11-8 下午12:03:11
 * @version 1.0.0
 */
public class CanalController {

    private static final Logger logger = LoggerFactory.getLogger(CanalController.class);

    // 对应canal.properties文件中的canal.id，目前无实际用途
    private Long cid;
    // 对应canal.properties文件中的canal.ip，canal server监听的ip。
    private String ip;
    // 对应canal.properties文件中的canal.port，canal server监听的端口
    private int port;

    // 默认使用spring的方式载入
    private Map<String, InstanceConfig> instanceConfigs; // instance实例配置
    private InstanceConfig globalInstanceConfig; // 全局配置
    private Map<String, CanalConfigClient> managerClients;

    // 监听instance config的变化
    private boolean autoScan = true; // 配置文件自动检测机制
    private InstanceAction defaultAction; //  如果配置发生了变更，默认应该采取什么样的操作
    private Map<InstanceMode, InstanceConfigMonitor> instanceConfigMonitors; // 对spring或manager这两种方式的配置加载方式都进行监听,所以这里是个map

    // CanalServer的两种启动方式，一种是内嵌，一种是独立部署
    private CanalServerWithEmbedded embededCanalServer;
    private CanalServerWithNetty canalServer;

    // canalInstance生成器
    private CanalInstanceGenerator instanceGenerator;

    private ZkClientx zkclientx;

    public CanalController() {
        this(System.getProperties());
    }

    public CanalController(final Properties properties) {
        managerClients = MigrateMap.makeComputingMap(new Function<String, CanalConfigClient>() {

            public CanalConfigClient apply(String managerAddress) {
                return getManagerClient(managerAddress);
            }
        });

        // *************************1、配置解析相关代码*******************************/
        // 初始化全局参数设置
        globalInstanceConfig = initGlobalConfig(properties);
        // 这里利用Google Guava框架的MapMaker创建Map实例并赋值给instanceConfigs
        instanceConfigs = new MapMaker().makeMap();
        // 初始化instance config
        initInstanceConfig(properties);


        // *************************2、准备canal server*******************************/
        // 准备canal server
        cid = Long.valueOf(getProperty(properties, CanalConstants.CANAL_ID));
        ip = getProperty(properties, CanalConstants.CANAL_IP);
        port = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_PORT));

        // CanalServerWithEmbedded --- 嵌入式版本实现
        embededCanalServer = new CanalServerWithEmbedded();
        embededCanalServer.setCanalInstanceGenerator(instanceGenerator);// 设置自定义的instanceGenerator

        // CanalServerWithNetty --- 基于netty网络服务的server实现
        canalServer = new CanalServerWithNetty(embededCanalServer);
        canalServer.setIp(ip);
        canalServer.setPort(port);

        // PS: 嵌入式版本实现: 说白了，就是我们可以不必独立部署canal server。在应用直接使用CanalServerWithEmbedded直连mysql数据库。
        // 而 CanalServerWithNetty是在CanalServerWithEmbedded的基础上做的一层封装，用于与客户端通信。
        // Canal客户端发送的所有请求都交给CanalServerWithNetty处理解析，解析完成之后委派给了交给CanalServerWithEmbedded进行处理。因此CanalServerWithNetty就是一个马甲而已。CanalServerWithEmbedded才是核心。
        // 在独立部署canal server时，Canal客户端发送的所有请求都交给CanalServerWithNetty处理解析，解析完成之后委派给了交给CanalServerWithEmbedded进行处理。
        // 因此CanalServerWithNetty就是一个马甲而已。CanalServerWithEmbedded才是核心。

        // 处理下ip为空，默认使用hostIp暴露到zk中
        if (StringUtils.isEmpty(ip)) {
            ip = AddressUtils.getHostIp();
        }

        // ************************3、初始化zk相关代码*******************************/
        // 读取canal.properties中的配置项canal.zkServers，如果没有这个配置，则表示项目不使用zk
        // canal支持利用了zk来完成HA机制、以及将当前消费到到的mysql的binlog位置记录到zk中。
        // ZkClientx是canal对ZkClient进行了一层简单的封装。显然，当我们没有配置canal.zkServers，那么zkclientx不会被初始化。
        final String zkServers = getProperty(properties, CanalConstants.CANAL_ZKSERVERS);
        if (StringUtils.isNotEmpty(zkServers)) {
            // 创建zk实例
            zkclientx = ZkClientx.getZkClient(zkServers);
            // 初始化系统目录
            // destination列表，路径为/otter/canal/destinations
            zkclientx.createPersistent(ZookeeperPathUtils.DESTINATION_ROOT_NODE, true);
            //整个canal server的集群列表，路径为/otter/canal/cluster
            zkclientx.createPersistent(ZookeeperPathUtils.CANAL_CLUSTER_ROOT_NODE, true);
        }

        //  ********************4、CanalInstance运行状态监控相关代码*******************/
        // ServerRunningMonitors 是 ServerRunningMonitor对象的容器
        // 而ServerRunningMonitor用于监控CanalInstance。
        // canal会为每一个destination创建一个CanalInstance。
        // 每个CanalInstance都会由一个ServerRunningMonitor来进行监控。
        // 而ServerRunningMonitor统一由ServerRunningMonitors进行管理。
        // 除了CanalInstance需要监控，CanalServer本身也需要监控。
        // 因此我们在代码一开始，就看到往ServerRunningMonitors设置了一个ServerRunningData对象，
        // 封装了canal server监听的ip和端口等信息。

        final ServerRunningData serverData = new ServerRunningData(cid, ip + ":" + port);

        // 1) 监控CanalServer本身
        ServerRunningMonitors.setServerData(serverData);

        // 2) ServerRunningMonitor统一由ServerRunningMonitors进行管理。
        // 往ServerRunningMonitors设置Map时, 通过MigrateMap.makeComputingMap方法来创建的，其接受一个Function类型的参数，这是guava中定义的接口，其声明了apply抽象方法
        ServerRunningMonitors.setRunningMonitors(MigrateMap.makeComputingMap(new Function<String, ServerRunningMonitor>() {

            // 3)每个CanalInstance都会由一个ServerRunningMonitor来进行监控
            public ServerRunningMonitor apply(final String destination) {
                ServerRunningMonitor runningMonitor = new ServerRunningMonitor(serverData);
                runningMonitor.setDestination(destination);
                runningMonitor.setListener(new ServerRunningListener() {

                    /**
                     内部调用了embededCanalServer的start(destination)方法。
                     此处需要划重点，说明每个destination对应的CanalInstance是通过embededCanalServer的start方法启动的，
                     这与我们之前分析将instanceGenerator设置到embededCanalServer中可以对应上。
                     embededCanalServer负责调用instanceGenerator生成CanalInstance实例，并负责其启动。
                     **/
                    public void processActiveEnter() {
                        try {
                            MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                            embededCanalServer.start(destination);
                        } finally {
                            MDC.remove(CanalConstants.MDC_DESTINATION);
                        }
                    }

                    /**
                     内部调用embededCanalServer的stop(destination)方法。与上start方法类似，只不过是停止CanalInstance。
                     **/
                    public void processActiveExit() {
                        try {
                            MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                            embededCanalServer.stop(destination);
                        } finally {
                            MDC.remove(CanalConstants.MDC_DESTINATION);
                        }
                    }

                    /**
                     处理存在zk的情况下，在Canalinstance启动之前，在zk中创建节点。
                     路径为：/otter/canal/destinations/{0}/cluster/{1}，其0会被destination替换，1会被ip:port替换。
                     此方法会在processActiveEnter()之前被调用
                     **/
                    public void processStart() {
                        try {
                            if (zkclientx != null) {
                                /**
                                 * 构建临时节点的路径：/otter/canal/destinations/{0}/running，其中占位符{0}会被destination替换。
                                 * 在集群模式下，可能会有多个canal server共同处理同一个destination，在某一时刻，只能由一个canal server进行处理，
                                 * 处理这个destination的canal server进入running状态，其他canal server进入standby状态。
                                 */
                                final String path = ZookeeperPathUtils.getDestinationClusterNode(destination, ip + ":" + port);
                                initCid(path);

                                /**
                                 * 对destination对应的running节点进行监听，一旦发生了变化，则说明可能其他处理相同destination的canal server可能出现了异常，
                                 * 此时需要尝试自己进入running状态。
                                 */
                                zkclientx.subscribeStateChanges(new IZkStateListener() {
                                    public void handleStateChanged(KeeperState state) throws Exception {

                                    }

                                    public void handleNewSession() throws Exception {
                                        // 尝试自己进入running状态
                                        initCid(path);
                                    }
                                });
                            }
                        } finally {
                            MDC.remove(CanalConstants.MDC_DESTINATION);
                        }
                    }

                    /**
                     处理存在zk的情况下，在Canalinstance停止前，释放zk节点
                     路径为/otter/canal/destinations/{0}/cluster/{1}，其0会被destination替换，1会被ip:port替换。
                     此方法会在processActiveExit()之前被调用
                     **/
                    public void processStop() {
                        try {
                            MDC.put(CanalConstants.MDC_DESTINATION, String.valueOf(destination));
                            if (zkclientx != null) {
                                final String path = ZookeeperPathUtils.getDestinationClusterNode(destination, ip + ":"
                                        + port);
                                releaseCid(path);
                            }
                        } finally {
                            MDC.remove(CanalConstants.MDC_DESTINATION);
                        }
                    }
                });
                if (zkclientx != null) {
                    runningMonitor.setZkClient(zkclientx);
                }
                return runningMonitor;
            }
        }));


        //  ********************5、autoScan机制相关代码******************* /
        // autoScan是否需要自动扫描的开关，只有当autoScan为true时，才会初始化defaultAction字段和instanceConfigMonitors字段。其中

        // defaultAction：其作用是如果配置发生了变更，默认应该采取什么样的操作。
        // 其实现了InstanceAction接口定义的三个抽象方法：start、stop和reload。当
        // 新增一个destination配置时，需要调用start方法来启动；当移除一个destination配置时，需要调用stop方法来停止；
        // 当某个destination配置发生变更时，需要调用reload方法来进行重启。

        // instanceConfigMonitors：类型为Map<InstanceMode, InstanceConfigMonitor>。
        //
        // defaultAction字段只是定义了配置发生变化默认应该采取的操作，那么总该有一个类来监听配置是否发生了变化，这就是InstanceConfigMonitor的作用。
        // 官方文档中，只提到了对canal.conf.dir配置项指定的目录的监听，这指的是通过spring方式加载配置。
        // 此时可以理解为什么instanceConfigMonitors的类型是一个Map，key为InstanceMode，就是为了对这两种方式的配置加载方式都进行监听。
        // 显然的，通过manager方式加载配置，配置中心的内容也是可能发生变化的，也需要进行监听。
        autoScan = BooleanUtils.toBoolean(getProperty(properties, CanalConstants.CANAL_AUTO_SCAN));
        if (autoScan) {

            // 初始化配置更新的默认操作
            defaultAction = new InstanceAction() {

                // 新增一个destination配置时，需要调用start方法来启动
                public void start(String destination) {
                    InstanceConfig config = instanceConfigs.get(destination);
                    if (config == null) {
                        // 重新读取一下instance config
                        config = parseInstanceConfig(properties, destination);
                        instanceConfigs.put(destination, config);
                    }

                    if (!config.getLazy() && !embededCanalServer.isStart(destination)) {
                        // HA机制启动
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (!runningMonitor.isStart()) {
                            runningMonitor.start();
                        }
                    }
                }

                // 当移除一个destination配置时，需要调用stop方法来停止；
                public void stop(String destination) {
                    // 此处的stop，代表强制退出，非HA机制，所以需要退出HA的monitor和配置信息
                    InstanceConfig config = instanceConfigs.remove(destination);
                    if (config != null) {
                        embededCanalServer.stop(destination);
                        ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                        if (runningMonitor.isStart()) {
                            runningMonitor.stop();
                        }
                    }
                }

                // 当某个destination配置发生变更时，需要调用reload方法来进行重启。
                public void reload(String destination) {
                    // 目前任何配置变化，直接重启，简单处理
                    stop(destination);
                    start(destination);
                }
            };


            // 初始化配置监听器
            instanceConfigMonitors = MigrateMap.makeComputingMap(new Function<InstanceMode, InstanceConfigMonitor>() {

                public InstanceConfigMonitor apply(InstanceMode mode) {
                    int scanInterval = Integer.valueOf(getProperty(properties, CanalConstants.CANAL_AUTO_SCAN_INTERVAL));
                    // 如果加载方式是spring，返回SpringInstanceConfigMonitor
                    if (mode.isSpring()) {
                        SpringInstanceConfigMonitor monitor = new SpringInstanceConfigMonitor();
                        monitor.setScanIntervalInSecond(scanInterval);
                        monitor.setDefaultAction(defaultAction);
                        // 设置conf目录，默认是user.dir + conf目录组成
                        String rootDir = getProperty(properties, CanalConstants.CANAL_CONF_DIR);
                        // 设置conf目录，默认是user.dir + conf目录组成
                        if (StringUtils.isEmpty(rootDir)) {
                            rootDir = "../conf";
                        }
                        monitor.setRootConf(rootDir);
                        return monitor;
                    } else if (mode.isManager()) {
                        // 如果加载方式是manager，返回ManagerInstanceConfigMonitor
                        return new ManagerInstanceConfigMonitor();
                    } else {
                        throw new UnsupportedOperationException("unknow mode :" + mode + " for monitor");
                    }
                }
            });
        }
    }

    // 表示canal instance的全局配置，类型为InstanceConfig，
    // 通过initGlobalConfig方法进行初始化。主要用于解析canal.properties以下几个配置项：
    private InstanceConfig initGlobalConfig(Properties properties) {

        InstanceConfig globalConfig = new InstanceConfig();

        // canal.instance.global.mode
        // 确定canal instance配置加载方式，取值有manager|spring两种方式
        String modeStr = getProperty(properties, CanalConstants.getInstanceModeKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(modeStr)) {
            // 将modelStr转成枚举InstanceMode，这是一个枚举类，只有2个取值，SPRING\MANAGER，对应两种配置方式
            globalConfig.setMode(InstanceMode.valueOf(StringUtils.upperCase(modeStr)));
        }

        // canal.instance.global.lazy：
        // 确定canal instance是否延迟初始化
        String lazyStr = getProperty(properties, CanalConstants.getInstancLazyKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(lazyStr)) {
            globalConfig.setLazy(Boolean.valueOf(lazyStr));
        }

        // canal.instance.global.manager.address：配置中心地址。
        // 如果canal.instance.global.mode=manager，需要提供此配置项
        String managerAddress = getProperty(properties, CanalConstants.getInstanceManagerAddressKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(managerAddress)) {
            globalConfig.setManagerAddress(managerAddress);
        }

        // canal.instance.global.spring.xml：spring配置文件路径。
        // 如果canal.instance.global.mode=spring，需要提供此配置项
        String springXml = getProperty(properties, CanalConstants.getInstancSpringXmlKey(CanalConstants.GLOBAL_NAME));
        if (StringUtils.isNotEmpty(springXml)) {
            globalConfig.setSpringXml(springXml);
        }

        // 创建CanalInstance生成器
        instanceGenerator = new CanalInstanceGenerator() {

            public CanalInstance generate(String destination) {
                // 1、根据destination从instanceConfigs获取对应的InstanceConfig对象
                InstanceConfig config = instanceConfigs.get(destination);
                if (config == null) {
                    throw new CanalServerException("can't find destination:{}");
                }

                //2、如果destination对应的InstanceConfig的mode是manager方式，使用ManagerCanalInstanceGenerator
                if (config.getMode().isManager()) {
                    ManagerCanalInstanceGenerator instanceGenerator = new ManagerCanalInstanceGenerator();
                    instanceGenerator.setCanalConfigClient(managerClients.get(config.getManagerAddress()));
                    return instanceGenerator.generate(destination);
                } else if (config.getMode().isSpring()) {
                    //3、如果destination对应的InstanceConfig的mode是spring方式，使用SpringCanalInstanceGenerator
                    SpringCanalInstanceGenerator instanceGenerator = new SpringCanalInstanceGenerator();
                    synchronized (this) {
                        try {
                            // 设置当前正在加载的通道，加载spring查找文件时会用到该变量
                            System.setProperty(CanalConstants.CANAL_DESTINATION_PROPERTY, destination);
                            // 使用xml方式创建的bean工厂
                            instanceGenerator.setBeanFactory(getBeanFactory(config.getSpringXml()));
                            return instanceGenerator.generate(destination);
                        } catch (Throwable e) {
                            logger.error("generator instance failed.", e);
                            throw new CanalException(e);
                        } finally {
                            System.setProperty(CanalConstants.CANAL_DESTINATION_PROPERTY, "");
                        }
                    }
                } else {
                    throw new UnsupportedOperationException("unknow mode :" + config.getMode());
                }

            }

        };

        return globalConfig;
    }

    private CanalConfigClient getManagerClient(String managerAddress) {
        return new CanalConfigClient();
    }

    private BeanFactory getBeanFactory(String springXml) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext(springXml);
        return applicationContext;
    }

    // 初始化instance的配置
    private void initInstanceConfig(Properties properties) {
        // 读取配置项canal.destinations
        String destinationStr = getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
        // 以","分割canal.destinations，得到一个数组形式的destination
        // 可以理解一个destination就对应要初始化一个canal instance。
        String[] destinations = StringUtils.split(destinationStr, CanalConstants.CANAL_DESTINATION_SPLIT);

        for (String destination : destinations) {
            // 为每一个destination生成一个InstanceConfig实例
            InstanceConfig config = parseInstanceConfig(properties, destination);
            // 将destination对应的InstanceConfig放入instanceConfigs中
            InstanceConfig oldConfig = instanceConfigs.put(destination, config);

            if (oldConfig != null) {
                logger.warn("destination:{} old config:{} has replace by new config:{}", new Object[]{destination,
                        oldConfig, config});
            }
        }
    }

    // 解析实例配置- 先用全局的，再用自己的覆盖
    private InstanceConfig parseInstanceConfig(Properties properties, String destination) {
        // 每个destination对应的InstanceConfig都引用了全局的globalInstanceConfig
        InstanceConfig config = new InstanceConfig(globalInstanceConfig);
        String modeStr = getProperty(properties, CanalConstants.getInstanceModeKey(destination));
        if (!StringUtils.isEmpty(modeStr)) {
            config.setMode(InstanceMode.valueOf(StringUtils.upperCase(modeStr)));
        }

        String lazyStr = getProperty(properties, CanalConstants.getInstancLazyKey(destination));
        if (!StringUtils.isEmpty(lazyStr)) {
            config.setLazy(Boolean.valueOf(lazyStr));
        }

        if (config.getMode().isManager()) {
            String managerAddress = getProperty(properties, CanalConstants.getInstanceManagerAddressKey(destination));
            if (StringUtils.isNotEmpty(managerAddress)) {
                config.setManagerAddress(managerAddress);
            }
        } else if (config.getMode().isSpring()) {
            String springXml = getProperty(properties, CanalConstants.getInstancSpringXmlKey(destination));
            if (StringUtils.isNotEmpty(springXml)) {
                config.setSpringXml(springXml);
            }
        }

        return config;
    }

    private String getProperty(Properties properties, String key) {
        return StringUtils.trim(properties.getProperty(StringUtils.trim(key)));
    }

    // CanalLauncher.start()-->CanalController.start()-->ServerRunningMonitor.start()
    public void start() throws Throwable {
        logger.info("## start the canal server[{}:{}]", ip, port);

        // 创建整个canal的工作节点
        final String path = ZookeeperPathUtils.getCanalClusterNode(ip + ":" + port);

        initCid(path);

        //
        if (zkclientx != null) {
            this.zkclientx.subscribeStateChanges(new IZkStateListener() {

                public void handleStateChanged(KeeperState state) throws Exception {

                }

                public void handleNewSession() throws Exception {
                    initCid(path);
                }
            });
        }

        // 优先启动embeded服务
        embededCanalServer.start();

        // 尝试启动一下非lazy状态的通道
        for (Map.Entry<String, InstanceConfig> entry : instanceConfigs.entrySet()) {
            final String destination = entry.getKey();
            InstanceConfig config = entry.getValue();
            // 创建destination的工作节点
            if (!config.getLazy() && !embededCanalServer.isStart(destination)) {
                // HA机制启动
                ServerRunningMonitor runningMonitor = ServerRunningMonitors.getRunningMonitor(destination);
                // 启动ServerRunningMonitor
                if (!runningMonitor.isStart()) {
                    runningMonitor.start();
                }
            }

            if (autoScan) {
                instanceConfigMonitors.get(config.getMode()).register(destination, defaultAction);
            }
        }

        // 启动配置文件自动检测机制
        if (autoScan) {
            instanceConfigMonitors.get(globalInstanceConfig.getMode()).start();
            for (InstanceConfigMonitor monitor : instanceConfigMonitors.values()) {
                if (!monitor.isStart()) {
                    monitor.start();
                }
            }
        }

        // 启动网络接口，监听客户端请求
        canalServer.start();
    }

    public void stop() throws Throwable {
        canalServer.stop();

        if (autoScan) {
            for (InstanceConfigMonitor monitor : instanceConfigMonitors.values()) {
                if (monitor.isStart()) {
                    monitor.stop();
                }
            }
        }

        for (ServerRunningMonitor runningMonitor : ServerRunningMonitors.getRunningMonitors().values()) {
            if (runningMonitor.isStart()) {
                runningMonitor.stop();
            }
        }

        // 释放canal的工作节点
        releaseCid(ZookeeperPathUtils.getCanalClusterNode(ip + ":" + port));
        logger.info("## stop the canal server[{}:{}]", ip, port);
    }

    private void initCid(String path) {
        // logger.info("## init the canalId = {}", cid);
        // 初始化系统目录
        if (zkclientx != null) {
            try {
                zkclientx.createEphemeral(path);
            } catch (ZkNoNodeException e) {
                // 如果父目录不存在，则创建
                String parentDir = path.substring(0, path.lastIndexOf('/'));
                zkclientx.createPersistent(parentDir, true);
                zkclientx.createEphemeral(path);
            }

        }
    }

    private void releaseCid(String path) {
        // logger.info("## release the canalId = {}", cid);
        // 初始化系统目录
        if (zkclientx != null) {
            zkclientx.delete(path);
        }
    }

}
