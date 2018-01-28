package me.libme.rec.cluster._trait;

import me.libme.fn.netty.server.HttpRequest;
import me.libme.kernel._c.util.JDateUtils;

import java.util.Date;

/**
 * Created by J on 2018/1/28.
 */
public class TimeGetDemo implements TimeGet {

    @Override
    public String time(String name, HttpRequest httpRequest) {
        return name + " - " + JDateUtils.formatWithSeconds(new Date());
    }


}
