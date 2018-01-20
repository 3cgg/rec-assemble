package me.libme.rec.receiver.model;

import me.libme.kernel._c._m.JModel;

/**
 * Created by J on 2018/1/17.
 */
public class UserItemRecord implements JModel {

    private String userId;

    private String itemId;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }
}
