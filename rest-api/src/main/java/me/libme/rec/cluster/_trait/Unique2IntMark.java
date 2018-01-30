package me.libme.rec.cluster._trait;

import me.libme.fn.netty.server.HttpRequest;
import me.libme.fn.netty.server.fn._dispatch.PathListener;

/**
 * Created by J on 2018/1/30.
 */
@FunctionalInterface
public interface Unique2IntMark extends PathListener {

    String PATH="/api/unique2int";

    int unique(String name, HttpRequest httpRequest);

}





