package me.libme.rec.cluster;

import me.libme.kernel._c.util.CliParams;
import me.libme.module.zookeeper.ZooKeeperConnector;
import me.libme.module.zookeeper.fn.ls.LeaderConfig;
import me.libme.module.zookeeper.fn.ls.LeaderNodeRegister;
import me.libme.module.zookeeper.fn.ls.NodeLeader;
import scalalg.me.libme.rec.RecRuntime;

/**
 * Created by J on 2018/1/28.
 */
public class Cluster {

    private final String[] args;

    public Cluster(String[] args) {
        this.args = args;
    }

    public void start() throws Exception{

        RecRuntime recRuntime=RecRuntime.builder().args(args).getOrCreate();

        //zookeeper
        ZooKeeperConnector.ZookeeperExecutor executor=recRuntime.zookeeperExecutor().get();

        LeaderConfig conf=new LeaderConfig();
        conf.setBasePath("/cluster");
        conf.setName("Cluster");
        new CliParams(args).toMap().forEach((key,value)->conf.put(key,value));

        // leader register
        LeaderNodeRegister leaderNodeRegister=new LeaderNodeRegister("Leader Register","/leader-info",executor);

        //start netty server
        NettyServer nettyServer=new NettyServer();

        NodeLeader nodeLeader=NodeLeader.builder()
                .conf(conf)
                .executor(executor)
                .addOpenResource(leaderNodeRegister)
                .addOpenResource(nettyServer)
                .addCloseResource(leaderNodeRegister)
                .addCloseResource(nettyServer)
                .build();

        nodeLeader.start();
    }

    public void shutdown() throws Exception{



    }





}
