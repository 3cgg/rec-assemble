package me.libme.rec.fn.u2i;

import me.libme.kernel._c.cache.JCacheService;
import me.libme.kernel._c.util.JStringUtils;
import scalalg.me.libme.module.hbase.HBaseConnector;

import java.util.Map;

/**
 * Created by J on 2018/1/26.
 */
public class HBaseRepo implements Unique2IntRepo {


    private final String tableName;

    private final String family;

    private final String intMark;

    private final HBaseConnector.HBaseExecutor hBaseExecutor;

    public HBaseRepo(String tableName, String family, String intMark, HBaseConnector.HBaseExecutor hBaseExecutor) {
        this.tableName = tableName;
        this.family = family;
        this.intMark = intMark;
        this.hBaseExecutor = hBaseExecutor;
    }

    @Override
    public Integer initialize(JCacheService<String, Integer> cacheService) {
        Map<String, String> data= hBaseExecutor.queryOperations().scan(tableName,family,intMark);
        int max=0;
        for(Map.Entry<String,String> entry:data.entrySet()){
            String key=entry.getKey();
            String value=entry.getValue();
            Integer intMark=Integer.valueOf(value);
            cacheService.put(key,intMark);
            max=Math.max(max,intMark);
        }
        return max;
    }


    @Override
    public void put(String key, Integer value) {
        hBaseExecutor.columnOperations().insert(tableName,family,intMark,key,value.toString());
    }

    @Override
    public void remove(String key) {
        hBaseExecutor.columnOperations().delete(tableName,family,intMark,key);
    }


    @Override
    public Integer get(String key) {
        String value=hBaseExecutor.columnOperations().get(tableName,family,intMark,key);
        return JStringUtils.isNullOrEmpty(value)?null:Integer.valueOf(value);
    }


}
