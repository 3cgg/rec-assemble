package me.libme.rec.fn.u2i;

import me.libme.kernel._c.cache.JCacheService;
import me.libme.kernel._c.cache.JMapCacheService;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by J on 2018/1/26.
 */
public class SingleVMUnique2Int implements IUnique2Int {

    private AtomicInteger atomicInteger=new AtomicInteger(0);

    private JCacheService<String,Integer> cacheService=new JMapCacheService<>();

    private Unique2IntRepo cacheInit;

    private Lock lock=new ReentrantLock();

    public SingleVMUnique2Int(Unique2IntRepo cacheInit) {
        atomicInteger.set(cacheInit.initialize(cacheService));
        this.cacheInit=cacheInit;
    }

    @Override
    public int toInt(String string) {

        Integer countMark;

        if(cacheService.contains(string)){
            countMark=cacheService.get(string);
        }else {
            try{
                lock.lock();
                countMark=atomicInteger.incrementAndGet();
                this.cacheInit.put(string,countMark);
                cacheService.put(string,countMark);
            }finally {
                lock.unlock();
            }

        }
        return countMark;
    }


}
