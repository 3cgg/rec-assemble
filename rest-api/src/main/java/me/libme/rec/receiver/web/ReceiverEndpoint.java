package me.libme.rec.receiver.web;

import me.libme.kernel._c.util.JStringUtils;
import me.libme.rec.receiver.QueueHolder;
import me.libme.rec.receiver.model.Content;
import me.libme.rec.receiver.model.Event;
import me.libme.rec.receiver.model.TrackData;
import me.libme.rec.receiver.model.UserItemRecord;
import me.libme.webboot.ResponseModel;
import me.libme.webseed.web.ClosureException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by J on 2018/1/17.
 */
@Controller
@RequestMapping("/receiver")
@ClosureException
public class ReceiverEndpoint {


    @Autowired
    private QueueHolder queueHolder;

    @ResponseBody
    @RequestMapping(path="/message",method= {RequestMethod.GET,RequestMethod.POST})
    public ResponseModel message (Event event, UserItemRecord userItemRecord, Content content) throws Exception {
        // do something validation on the contentRecord.

        if(JStringUtils.isNullOrEmpty(event.getSource())
                ||event.getTime()==null
                ||event.getType()==null
                ||JStringUtils.isNullOrEmpty(userItemRecord.getUserId())
                ||JStringUtils.isNullOrEmpty(userItemRecord.getItemId())
                ||JStringUtils.isNullOrEmpty(content.getDesc())
                ||JStringUtils.isNullOrEmpty(content.getData())) {
            return ResponseModel.newBysError("any content should not be empty");
        }


        TrackData trackData=new TrackData();
        trackData.setEvent(event);
        trackData.setUserItemRecord(userItemRecord);
        trackData.setContent(content);

        queueHolder.queue().offer(trackData);

        return ResponseModel.newSuccess(true);
    }


    @ResponseBody
    @RequestMapping(path="/status",method= {RequestMethod.GET})
    public ResponseModel status () throws Exception {

        Map<String,Object> info=new HashMap<>();
        info.put("queueSize",queueHolder.queue().size());

        return ResponseModel.newSuccess(info);

    }






}
