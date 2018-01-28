package me.libme.rec.cluster._trait;

import me.libme.fn.netty.server.HttpRequest;
import me.libme.fn.netty.server.fn._dispatch.PathListener;
import me.libme.kernel._c.util.JDateUtils;

import java.util.Date;

/**
 * Created by J on 2018/1/28.
 */
public interface TimeGet extends PathListener {

    String PATH="/demo/time";

    String time(String name, HttpRequest httpRequest);


    class TimeGetDemo implements TimeGet{

        @Override
        public String time(String name, HttpRequest httpRequest) {
            return name+" - "+ JDateUtils.formatWithSeconds(new Date());
        }


    }

}
