package me.libme.rec.cluster;

import me.libme.fn.netty.client.ClientChannelExecutor;
import me.libme.fn.netty.client.DynamicClientChannelExecutor;
import me.libme.fn.netty.client.SimpleClient;
import me.libme.module.zookeeper.ZooKeeperConnector;
import scalalg.me.libme.rec.RecRuntime;

import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by J on 2018/1/30.
 */
public class PathListenerClientFactory {



    private static AtomicReference<ClientChannelExecutor> clientChannelExecutorHolder=new AtomicReference<>();

    private static Lock lock=new ReentrantLock();

    public static  <T> T factory(Class<T> intarface,String path){

        if(clientChannelExecutorHolder.get()==null){
            try{
                lock.lock();

                if(clientChannelExecutorHolder.get()==null){
                    RecRuntime recRuntime=RecRuntime.builder().getOrCreate();

                    //zookeeper
                    ZooKeeperConnector.ZookeeperExecutor executor=recRuntime.zookeeperExecutor().get();

                    ChannelInfoOnZkProvider channelInfoOnZkProvider=new ChannelInfoOnZkProvider(ClusterZkPaths.LEADER_INFO_PATH,executor);
                    ClientChannelExecutor clientChannelExecutor=new DynamicClientChannelExecutor(channelInfoOnZkProvider);
                    clientChannelExecutorHolder.set(clientChannelExecutor);
                }

            }finally {
                lock.unlock();
            }

        }

        SimpleClient simpleClient=new SimpleClient(clientChannelExecutorHolder.get(),path);
        return (T) Proxy.newProxyInstance(intarface.getClassLoader(),new Class[]{intarface},simpleClient);

    }




}
