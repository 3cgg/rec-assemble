package me.libme.rec.fn.u2i;

import me.libme.kernel._c.cache.JCacheService;

/**
 * Created by J on 2018/1/26.
 */
public interface Unique2IntRepo {

    /**
     *
     * @param cacheService
     * @return max integer
     */
    Integer initialize(JCacheService<String, Integer> cacheService);

    void put(String key,Integer value);

    void remove(String key);

}
