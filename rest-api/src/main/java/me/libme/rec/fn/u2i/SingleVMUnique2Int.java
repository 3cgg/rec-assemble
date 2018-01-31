package me.libme.rec.fn.u2i;

import me.libme.kernel._c.cache.JCacheService;
import me.libme.kernel._c.cache.JMapCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by J on 2018/1/26.
 */
public class SingleVMUnique2Int implements IUnique2Int {

    private static final Logger LOGGER= LoggerFactory.getLogger(SingleVMUnique2Int.class);


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
                boolean exception=false;
                try {
                    this.cacheInit.put(string,countMark);
                }catch (Throwable e){
                    exception=true;
                    LOGGER.error(e.getMessage(),e);
                    Integer value=null;
                    try{
                        value=this.cacheInit.get(string);  //check again
                    }catch (Throwable e1){
                        LOGGER.error(e1.getMessage(),e1);
                    }
                    if(value==null||value.compareTo(atomicInteger.get())!=0)
                        atomicInteger=new AtomicInteger(atomicInteger.get());
                    else if(value.compareTo(atomicInteger.get())==0) exception=false;
                }

                Integer value=null;
                try{
                    value=this.cacheInit.get(string);  //check again
                }catch (Throwable e1){
                    LOGGER.error(e1.getMessage(),e1);
                }

                if(value==null||value.compareTo(atomicInteger.get())!=0) exception=true;

                if(!exception)
                    cacheService.put(string,countMark);
                else{
                    atomicInteger=new AtomicInteger(atomicInteger.get());
                }
            }finally {
                lock.unlock();
            }

        }
        return countMark;
    }


}
