package me.libme.rec.fn.u2i;

import me.libme.kernel._c.cache.JCacheService;
import me.libme.kernel._c.cache.JMapCacheService;
import me.libme.rec.cluster.PathListenerClientFactory;
import me.libme.rec.cluster._trait.Unique2IntMark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scalalg.me.libme.rec.RecRuntime;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by J on 2018/2/1.
 */
public class ReadOnlyU2I implements IUnique2Int{

    private static final Logger LOGGER= LoggerFactory.getLogger(ReadOnlyU2I.class);

    private JCacheService<String,Integer> cacheService=new JMapCacheService<>();

    private static ReadOnlyU2I instance;

    public static ReadOnlyU2I get(){

        if(instance==null){
            synchronized (ReadOnlyU2I.class){
                if(instance==null){
                    RecRuntime recRuntime=RecRuntime.builder().getOrCreate();
                    Unique2IntRepo unique2IntRepo=new HBaseRepo(recRuntime.u2iTable(),recRuntime.u2iFamily()
                            ,recRuntime.u2iColumn(),recRuntime.hbaseExecutor().get());
                    instance=new ReadOnlyU2I();
                    int max=unique2IntRepo.initialize(instance.cacheService);
                    LOGGER.info("worker collect all elements of u2i collection, the max value : "+max);
                }
            }
        }

        return instance;
    }

    private Lock lock=new ReentrantLock();

    private ReadOnlyU2I(){}



    @Override
    public int toInt(String string) {

        Integer iVal;

        if((iVal=cacheService.get(string))==null){

            try{
                lock.lock();
                if((iVal=cacheService.get(string))==null){
                    Unique2IntMark unique2IntMark= PathListenerClientFactory.factory(Unique2IntMark.class,Unique2IntMark.PATH);
                    iVal=unique2IntMark.unique(string,null);
                    cacheService.put(string,iVal);
                }
            }finally {
                lock.unlock();
            }
        }
        return iVal;

    }


}
