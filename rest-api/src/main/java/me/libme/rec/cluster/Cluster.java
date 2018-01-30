package me.libme.rec.cluster;

import me.libme.fn.netty.server.fn._dispatch.PathListenerInitializeQueue;
import me.libme.kernel._c.util.CliParams;
import me.libme.module.zookeeper.ZooKeeperConnector;
import me.libme.module.zookeeper.fn.ls.LeaderConfig;
import me.libme.module.zookeeper.fn.ls.LeaderNodeRegister;
import me.libme.module.zookeeper.fn.ls.NodeLeader;
import me.libme.module.zookeeper.fn.ls.OpenResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scalalg.me.libme.rec.RecRuntime;

/**
 * Created by J on 2018/1/28.
 */
public class Cluster {

    private static final Logger LOGGER= LoggerFactory.getLogger(Cluster.class);


    private final String[] args;

    public Cluster(String[] args) {
        this.args = args;
    }

    public void start() throws Exception{
        LOGGER.info("start cluster...");
        RecRuntime recRuntime=RecRuntime.builder().args(args).getOrCreate();

        //zookeeper
        ZooKeeperConnector.ZookeeperExecutor executor=recRuntime.zookeeperExecutor().get();

        LeaderConfig conf=new LeaderConfig();
        conf.setBasePath(ClusterZkPaths.BASE_PATH);
        conf.setName("Cluster");
        new CliParams(args).toMap().forEach((key,value)->conf.put(key,value));

        // leader register
        LeaderNodeRegister leaderNodeRegister=new LeaderNodeRegister("Leader Register",ClusterZkPaths.LEADER_INFO_PATH,executor);

        //start netty server
        NettyServer nettyServer=new NettyServer();

        NodeLeader nodeLeader=NodeLeader.builder()
                .conf(conf)
                .executor(executor)
                .addOpenResource(leaderNodeRegister)
                .addOpenResource(nettyServer)
                .addOpenResource(new OpenResource() {
                    @Override
                    public void open(NodeLeader nodeLeader) throws Exception {
                        PathListenerInitializeQueue.get().allInitialize();
                    }

                    @Override
                    public String name() {
                        return "Initialize Path Listener Object...";
                    }
                })
//                .addCloseResource(leaderNodeRegister)  //comment,avoid removing the path another node register itself
                .addCloseResource(nettyServer)
                .build();

        nodeLeader.start();

        LOGGER.info("start cluster OK!");
    }

    public void shutdown() throws Exception{



    }





}
