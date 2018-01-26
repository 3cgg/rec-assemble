package me.libme.rec.fn.uc;

import me.libme.kernel._c.cache.JCacheService;
import scalalg.me.libme.module.hbase.HBaseConnector;

import java.util.Map;

/**
 * Created by J on 2018/1/26.
 */
public class HBaseCacheInit implements CacheInit {


    private String tableName;

    private String family;

    private String intMark;

    private HBaseConnector.HBaseExecutor hBaseExecutor;


    @Override
    public void initialize(JCacheService<String, Integer> cacheService) {

        Map<String, String> data= hBaseExecutor.queryOperations().scan(tableName,family,intMark);




    }



}
