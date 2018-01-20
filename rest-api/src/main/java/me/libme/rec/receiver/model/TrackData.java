package me.libme.rec.receiver.model;

import me.libme.kernel._c._m.JModel;
import me.libme.kernel._c.json.JJSONObject;

import java.util.Date;

/**
 * Created by J on 2018/1/19.
 */
public class TrackData implements JModel ,JJSONObject {

    private Event event;

    private UserItemRecord userItemRecord;

    private Content content;

    public Event getEvent() {
        return event;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public UserItemRecord getUserItemRecord() {
        return userItemRecord;
    }

    public void setUserItemRecord(UserItemRecord userItemRecord) {
        this.userItemRecord = userItemRecord;
    }

    public Content getContent() {
        return content;
    }

    public void setContent(Content content) {
        this.content = content;
    }


    @Override
    public Object serializableJSONObject() {
        String _data=
                event.getType().name()+","+event.getTime().getTime()+","+event.getSource()
                +"|"+userItemRecord.getUserId()+","+userItemRecord.getItemId()
                +"|"+content.getDesc()+","+content.getData();
        return _data;
    }


    @Override
    public TrackData deserialize(String string) {

        String[] val=string.split("|");
        String[] event=val[0].split(","); //event
        Event ev=new Event();
        ev.setType(EventType.valueOf(event[0]));
        ev.setTime(new Date(Long.parseLong(event[1])));
        ev.setSource(event[2]);
        this.event=ev;

        UserItemRecord ui=new UserItemRecord();
        String[] uia=val[1].split(","); //user item
        ui.setUserId(uia[0]);
        ui.setItemId(uia[1]);
        this.userItemRecord=ui;

        Content cnt=new Content();
        String[] cnta=val[2].split(","); //content
        cnt.setDesc(cnta[0]);
        cnt.setData(cnta[1]);
        this.content=cnt;

        return this;
    }






}
