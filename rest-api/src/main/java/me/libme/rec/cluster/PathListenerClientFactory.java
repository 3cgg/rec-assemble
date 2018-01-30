package me.libme.rec.cluster;

import me.libme.fn.netty.client.ClientChannelExecutor;
import me.libme.fn.netty.client.DynamicClientChannelExecutor;
import me.libme.fn.netty.client.SimpleClient;
import me.libme.module.zookeeper.ZooKeeperConnector;
import scalalg.me.libme.rec.RecRuntime;

import java.lang.reflect.Proxy;

/**
 * Created by J on 2018/1/30.
 */
public class PathListenerClientFactory {


    public static  <T> T factory(Class<T> intarface,String path){

        RecRuntime recRuntime=RecRuntime.builder().getOrCreate();

        //zookeeper
        ZooKeeperConnector.ZookeeperExecutor executor=recRuntime.zookeeperExecutor().get();

        ChannelInfoOnZkProvider channelInfoOnZkProvider=new ChannelInfoOnZkProvider(ClusterZkPaths.LEADER_INFO_PATH,executor);
        ClientChannelExecutor clientChannelExecutor=new DynamicClientChannelExecutor(channelInfoOnZkProvider);

        SimpleClient simpleClient=new SimpleClient(clientChannelExecutor,path);
        return (T) Proxy.newProxyInstance(intarface.getClassLoader(),new Class[]{intarface},simpleClient);

    }




}
