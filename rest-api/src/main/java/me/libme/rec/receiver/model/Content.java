package me.libme.rec.receiver.model;

import me.libme.kernel._c._m.JModel;

/**
 * Created by J on 2018/1/17.
 */
public class Content implements JModel {

    private String desc;

    private String data;

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }
}
