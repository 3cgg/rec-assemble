package me.libme.rec.cluster._trait;

import me.libme.fn.netty.server.HttpRequest;
import me.libme.fn.netty.server.fn._dispatch.PathListenerInitializeQueue;
import me.libme.rec.fn.u2i.HBaseRepo;
import me.libme.rec.fn.u2i.SingleVMUnique2Int;
import me.libme.rec.fn.u2i.Unique2IntRepo;
import scalalg.me.libme.rec.RecRuntime;

/**
 * Created by J on 2018/1/30.
 */
public class Unique2IntMarkImpl implements Unique2IntMark {


    private SingleVMUnique2Int singleVMUnique2Int;

    public Unique2IntMarkImpl() {

        PathListenerInitializeQueue.get().offer(()->{

            RecRuntime recRuntime=RecRuntime.builder().getOrCreate();
            Unique2IntRepo unique2IntRepo=new HBaseRepo(recRuntime.u2iTable(),recRuntime.u2iFamily()
            ,recRuntime.u2iColumn(),recRuntime.hbaseExecutor().get());
            SingleVMUnique2Int singleVMUnique2Int=new SingleVMUnique2Int(unique2IntRepo);
            this.singleVMUnique2Int=singleVMUnique2Int;
        });

    }

    @Override
    public int unique(String name, HttpRequest httpRequest) {
        return singleVMUnique2Int.toInt(name);
    }


}
