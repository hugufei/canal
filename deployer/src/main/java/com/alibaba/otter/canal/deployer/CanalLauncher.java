package com.alibaba.otter.canal.deployer;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * canal独立版本启动的入口类
 * 
 * @author jianghang 2012-11-6 下午05:20:49
 * @version 1.0.0
 */

// deployer模块的主要作用：
// 1、读取canal.properties，确定canal instance的配置加载方式
// 2、确定canal instance的启动方式：独立启动或者集群方式启动
// 3、监听canal instance的配置的变化，动态停止、启动或新增
// 4、启动canal server，监听客户端请求

public class CanalLauncher {

    private static final String CLASSPATH_URL_PREFIX = "classpath:";
    private static final Logger logger               = LoggerFactory.getLogger(CanalLauncher.class);

    public static void main(String[] args) throws Throwable {
        try {

            //1、读取canal.properties文件中配置，默认读取classpath下的canal.properties
            String conf = System.getProperty("canal.conf", "classpath:canal.properties");
            Properties properties = new Properties();
            if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
                conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
                properties.load(CanalLauncher.class.getClassLoader().getResourceAsStream(conf));
            } else {
                properties.load(new FileInputStream(conf));
            }

            //2、启动canal，首先将properties对象传递给CanalController，然后调用其start方法启动
            logger.info("## start the canal server.");
            final CanalController controller = new CanalController(properties);
            //启动controller
            controller.start();
            logger.info("## the canal server is running now ......");

            //3、关闭canal，通过添加JVM的钩子，JVM停止前会回调run方法，其内部调用controller.stop()方法进行停止
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        logger.info("## stop the canal server");
                        controller.stop();
                    } catch (Throwable e) {
                        logger.warn("##something goes wrong when stopping canal Server:\n{}",
                            ExceptionUtils.getFullStackTrace(e));
                    } finally {
                        logger.info("## canal server is down.");
                    }
                }

            });
        } catch (Throwable e) {
            logger.error("## Something goes wrong when starting up the canal Server:\n{}",
                ExceptionUtils.getFullStackTrace(e));
            System.exit(0);
        }
    }
}
