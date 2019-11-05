package com.alibaba.otter.canal.server.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.server.CanalServer;
import com.alibaba.otter.canal.server.embedded.CanalServerWithEmbedded;
import com.alibaba.otter.canal.server.netty.handler.ClientAuthenticationHandler;
import com.alibaba.otter.canal.server.netty.handler.FixedHeaderFrameDecoder;
import com.alibaba.otter.canal.server.netty.handler.HandshakeInitializationHandler;
import com.alibaba.otter.canal.server.netty.handler.SessionHandler;

/**
 * 基于netty网络服务的server实现
 *
 * @author jianghang 2012-7-12 下午01:34:49
 * @version 1.0.0
 */
public class CanalServerWithNetty extends AbstractCanalLifeCycle implements CanalServer {

    // 监听的所有客户端请求都会为派给CanalServerWithEmbedded处理
    private CanalServerWithEmbedded embeddedServer;      // 嵌入式server

    // 监听的ip和port，client通过此ip和port与服务端通信
    private String ip;
    private int port;

    // netty组件
    private Channel serverChannel = null;
    private ServerBootstrap bootstrap = null;

    public CanalServerWithNetty() {
    }

    // 单例模式实现
    public CanalServerWithNetty(CanalServerWithEmbedded embeddedServer) {
        // 给embeddedServer赋值
        this.embeddedServer = embeddedServer;
    }

    // 启动入口，包含了netty启动的核心逻辑
    public void start() {
        super.start();

        // 优先启动内嵌的canal server，因为基于netty的实现需要将请求委派给其处理
        if (!embeddedServer.isStart()) {
            embeddedServer.start();
        }

        /*
            创建bootstrap实例，参数NioServerSocketChannelFactory也是Netty的API，其接受2个线程池参数
            其中第一个线程池是Accept线程池，第二个线程池是woker线程池，
            Accept线程池接收到client连接请求后，会将代表client的对象转发给worker线程池处理。
            这里属于netty的知识，不熟悉的用户暂时不必深究，简单认为netty使用线程来处理客户端的高并发请求即可。
         */
        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool()));

        // 构造对应的pipeline
        /*
            pipeline实际上就是netty对客户端请求的处理器链，
            可以类比JAVA EE编程中Filter的责任链模式，上一个filter处理完成之后交给下一个filter处理，
            只不过在netty中，不再是filter，而是ChannelHandler。
        */

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {

            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipelines = Channels.pipeline();
                // 主要是处理编码、解码。因为网路传输的传入的都是二进制流，FixedHeaderFrameDecoder的作用就是对其进行解析
                pipelines.addLast(FixedHeaderFrameDecoder.class.getName(), new FixedHeaderFrameDecoder());
                // 处理client与server握手
                pipelines.addLast(HandshakeInitializationHandler.class.getName(), new HandshakeInitializationHandler());
                // client身份验证
                pipelines.addLast(ClientAuthenticationHandler.class.getName(), new ClientAuthenticationHandler(embeddedServer));

                // SessionHandler用于真正的处理客户端请求
                SessionHandler sessionHandler = new SessionHandler(embeddedServer);
                pipelines.addLast(SessionHandler.class.getName(), sessionHandler);
                return pipelines;
            }
        });

        // 启动，当bind方法被调用时，netty开始真正的监控某个端口，此时客户端对这个端口的请求可以被接受到
        if (StringUtils.isNotEmpty(ip)) {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.ip, this.port));
        } else {
            this.serverChannel = bootstrap.bind(new InetSocketAddress(this.port));
        }
    }

    public void stop() {
        super.stop();

        if (this.serverChannel != null) {
            this.serverChannel.close().awaitUninterruptibly(1000);
        }

        if (this.bootstrap != null) {
            this.bootstrap.releaseExternalResources();
        }

        if (embeddedServer.isStart()) {
            embeddedServer.stop();
        }
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setEmbeddedServer(CanalServerWithEmbedded embeddedServer) {
        this.embeddedServer = embeddedServer;
    }

}
