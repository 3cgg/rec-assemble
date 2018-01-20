package me.libme.rec.receiver.web;

import me.libme.rec.receiver.QueueHolder;
import me.libme.rec.receiver.model.TrackData;
import org.springframework.stereotype.Component;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by J on 2018/1/20.
 */
@Component
public class ArrayBlockingQueueHolder implements QueueHolder {

    private ArrayBlockingQueue<TrackData> queue=new ArrayBlockingQueue<>(100000);


    @Override
    public Queue<TrackData> queue() {
        return queue;
    }
}
