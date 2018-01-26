package me.libme.rec.fn.uc;

import me.libme.kernel._c.cache.JCacheService;

/**
 * Created by J on 2018/1/26.
 */
interface CacheInit {

    void initialize(JCacheService<String, Integer> cacheService);

}
