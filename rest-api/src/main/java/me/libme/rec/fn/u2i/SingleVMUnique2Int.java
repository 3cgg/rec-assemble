package me.libme.rec.fn.u2i;

import me.libme.kernel._c.cache.JCacheService;
import me.libme.kernel._c.cache.JMapCacheService;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by J on 2018/1/26.
 */
public class SingleVMUnique2Int implements IUnique2Int {

    private AtomicInteger atomicInteger=new AtomicInteger(0);

    private JCacheService<String,Integer> cacheService=new JMapCacheService<>();


    public SingleVMUnique2Int(Unique2IntRepo cacheInit) {
        cacheInit.initialize(cacheService);
    }

    public SingleVMUnique2Int(Map<String,Integer> data) {
        data.forEach((key,value)->cacheService.put(key,value));
    }

    @Override
    public int toInt(String string) {

        Integer countMark=null;

        if(cacheService.contains(string)){
            countMark=cacheService.get(string);
        }else {
            countMark=atomicInteger.incrementAndGet();
            cacheService.put(string,countMark);
        }
        return countMark;
    }


}
