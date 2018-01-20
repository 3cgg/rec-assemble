package me.libme.rec.receiver;

import me.libme.rec.receiver.model.TrackData;

import java.util.Queue;

/**
 * Created by J on 2018/1/20.
 */
public interface QueueHolder {

    Queue<TrackData> queue();

}
