package me.libme.rec.receiver.web;

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
    @RequestMapping(path="/message",method= RequestMethod.POST)
    public ResponseModel message (Event event, UserItemRecord userItemRecord, Content content) throws Exception {
        // do something validation on the contentRecord.

        TrackData trackData=new TrackData();
        trackData.setEvent(event);
        trackData.setUserItemRecord(userItemRecord);
        trackData.setContent(content);

        queueHolder.queue().offer(trackData);

        return ResponseModel.newSuccess(true);
    }




}
