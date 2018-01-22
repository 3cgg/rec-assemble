package me.libme.recsystem.ml;

import scala.me.libme.recsystem.ml.UserItemStruct;

import java.util.List;

/**
 * Created by J on 2018/1/9.
 */
public interface RatingPersist {


    void persist(List<UserItemStruct> ratings) throws Exception;


}
