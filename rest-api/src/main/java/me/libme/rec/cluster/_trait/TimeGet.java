package me.libme.rec.cluster._trait;

import me.libme.fn.netty.server.HttpRequest;
import me.libme.fn.netty.server.fn._dispatch.PathListener;

/**
 * Created by J on 2018/1/28.
 */
public interface TimeGet extends PathListener {

    String PATH="/demo/time";

    String time(String name, HttpRequest httpRequest);


}
