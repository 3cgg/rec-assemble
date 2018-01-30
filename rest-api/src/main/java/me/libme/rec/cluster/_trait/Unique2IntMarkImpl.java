package me.libme.rec.cluster._trait;

import me.libme.fn.netty.server.HttpRequest;
import me.libme.rec.fn.u2i.SingleVMUnique2Int;

/**
 * Created by J on 2018/1/30.
 */
public class Unique2IntMarkImpl implements Unique2IntMark {


    private SingleVMUnique2Int singleVMUnique2Int;




    @Override
    public int unique(String name, HttpRequest httpRequest) {
        return 0;
    }


}
