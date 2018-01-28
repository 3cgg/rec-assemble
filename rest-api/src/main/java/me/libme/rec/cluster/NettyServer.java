package me.libme.rec.cluster;

import me.libme.fn.netty.server.HttpRequest;
import me.libme.fn.netty.server.ServerConfig;
import me.libme.fn.netty.server.SimpleHttpNioChannelServer;
import me.libme.fn.netty.server.fn._dispatch.PathListener;
import me.libme.fn.netty.server.fn._dispatch.SimpleRequestMappingDispatcher;
import me.libme.kernel._c._i.JParser;
import me.libme.kernel._c.json.JJSON;
import me.libme.kernel._c.util.JIOUtils;
import me.libme.kernel._c.util.JStringUtils;
import me.libme.module.zookeeper.fn.ls.CloseResource;
import me.libme.module.zookeeper.fn.ls.NodeLeader;
import me.libme.module.zookeeper.fn.ls.OpenResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by J on 2018/1/28.
 */
public class NettyServer implements OpenResource ,CloseResource {

    private static final Logger LOGGER= LoggerFactory.getLogger(NettyServer.class);

    private ServerConfig serverConfig;

    private SimpleHttpNioChannelServer channelServer;

    private RecNettyConfigParser recNettyConfigParser=new RecNettyConfigParser();

    @Override
    public void close(NodeLeader nodeLeader) throws IOException {
        channelServer.close();
    }

    @Override
    public void open(NodeLeader nodeLeader) throws Exception {

        serverConfig=recNettyConfigParser.parse(nodeLeader);

        SimpleRequestMappingDispatcher dispatcher=new SimpleRequestMappingDispatcher();

        dispatcher.register("/demo/_test4netty_/name", new PathListener() {
            public String name(String name){
                return name;
            }
        }).register("/demo/info", new PathListener() {
            public String info(String name, int age,HttpRequest httpRequest){
                httpRequest.paramNames().forEach(key-> LOGGER.info(key));
                return name+"-"+age;
            }
        });

        Map<String,Object> config = JJSON.get().parse(JStringUtils.utf8(JIOUtils.getBytes(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("rec-assemble.json"))));

        config.forEach((key,value)->{
            PathListener object;
            try {
                object= (PathListener) Class.forName(String.valueOf(value)).newInstance();
            } catch (Throwable e) {
                throw new IllegalStateException(e);
            }
            dispatcher.register(key,object);
        });

        // START SERVER
        channelServer =
                new SimpleHttpNioChannelServer(serverConfig,dispatcher);
        try {
            channelServer.start();
            LOGGER.info("Host ["+serverConfig.getHost()+"] listen on port : "+serverConfig.getPort()+", params : "+ JJSON.get().format(serverConfig));
        } catch (Exception e) {
            LOGGER.error( e.getMessage()+", params : "+ JJSON.get().format(serverConfig),e);
            try {
                channelServer.close();
            } catch (IOException e1) {
                LOGGER.error( e1.getMessage()+", params : "+ JJSON.get().format(serverConfig),e1);
                throw new RuntimeException(e);
            }
            throw new RuntimeException(e);
        }

    }

    @Override
    public String name() {
        return "Netty Server";
    }



    private class RecNettyConfigParser implements JParser {

        private static final String HOST="--rec.netty.host";

        private static final String PORT="--rec.netty.port";


        ServerConfig parse(NodeLeader nodeLeader){

            ServerConfig serverConfig=new ServerConfig();
            serverConfig.setHost((String) nodeLeader.get(HOST,"127.0.0.1"));
            serverConfig.setPort((Integer)nodeLeader.get(PORT,10089));
            serverConfig.setLoopThread(1);
            serverConfig.setWorkerThread(Runtime.getRuntime().availableProcessors());

            return serverConfig;
        }

    }








}
