package me.libme.rec.receiver.model;

import java.util.Date;

/**
 * Created by J on 2018/1/17.
 */
public class Event {

    private String source;

    private Date time;

    /**
     * click , browser  , etc.
     */
    private EventType type;


    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public EventType getType() {
        return type;
    }

    public void setType(EventType type) {
        this.type = type;
    }
}
