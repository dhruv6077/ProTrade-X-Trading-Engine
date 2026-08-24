package exchange.fix;

import exchange.dispatch.EventListener;
import exchange.gateway.OrderGateway;
import exchange.model.ExchangeEvent;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class NettyFixServer implements AutoCloseable, EventListener {
    private static final Logger logger = LoggerFactory.getLogger(NettyFixServer.class);

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final Channel serverChannel;
    private final List<Channel> activeSessions = new ArrayList<>();
    private final OrderGateway gateway;

    public NettyFixServer(OrderGateway gateway, int port) {
        this.gateway = gateway;

        boolean useEpoll = Epoll.isAvailable();
        this.bossGroup = useEpoll ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
        this.workerGroup = useEpoll ? new EpollEventLoopGroup(2) : new NioEventLoopGroup(2);

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group(bossGroup, workerGroup)
                .channel(useEpoll ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new FixFrameDecoder());
                        ch.pipeline().addLast(new FixSessionHandler(gateway));
                        synchronized (activeSessions) {
                            activeSessions.add(ch);
                        }
                        ch.closeFuture().addListener(f -> {
                            synchronized (activeSessions) {
                                activeSessions.remove(ch);
                            }
                        });
                    }
                })
                .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

        if (useEpoll) {
            bootstrap.childOption(EpollChannelOption.SO_BUSY_POLL, 50);
        }

        try {
            ChannelFuture future = bootstrap.bind(port).sync();
            this.serverChannel = future.channel();
            logger.info("Netty FIX Server started on port {}", port);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to bind FIX server", e);
        }
    }

    @Override
    public void onEvents(List<ExchangeEvent> events) {
        synchronized (activeSessions) {
            for (Channel ch : activeSessions) {
                if (ch.isActive()) {
                    for (ExchangeEvent event : events) {
                        FixEncoder.sendExecutionReport(ch.pipeline().lastContext(), event);
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }
}
