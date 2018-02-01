package me.libme.rec.fn.u2i;

import me.libme.kernel._c.cache.JCacheService;
import me.libme.kernel._c.cache.JMapCacheService;
import me.libme.rec.cluster.PathListenerClientFactory;
import me.libme.rec.cluster._trait.Unique2IntMark;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by J on 2018/2/1.
 */
public class ReadOnlyU2I implements IUnique2Int{


    private static JCacheService<String,Integer> cacheService=new JMapCacheService<>();

    private static final ReadOnlyU2I instance=new ReadOnlyU2I();

    public static ReadOnlyU2I get(){return instance;}

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
