package me.libme.rec.receiver.vo;

import me.libme.kernel._c._m.JModel;

/**
 * Created by J on 2018/1/17.
 */
public enum  EventType implements JModel {

    click("click") , browser("browser");

    private String name;

    EventType(String name) {
        this.name = name;
    }
}
