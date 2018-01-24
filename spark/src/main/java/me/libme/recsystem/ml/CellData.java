package me.libme.recsystem.ml;

import me.libme.kernel._c._m.JModel;

/**
 * Created by J on 2018/1/24.
 */
public class CellData implements JModel {

    private Double rating;

    private Long timestamp;

    public Double getRating() {
        return rating;
    }

    public void setRating(Double rating) {
        this.rating = rating;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public static CellData _default(){
        CellData rating=new CellData();
        rating.rating=0d;
        rating.timestamp=System.currentTimeMillis();
        return rating;
    }

}
