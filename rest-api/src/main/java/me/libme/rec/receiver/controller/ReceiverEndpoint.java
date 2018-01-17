package me.libme.rec.receiver.controller;

import me.libme.rec.receiver.vo.Content;
import me.libme.rec.receiver.vo.Event;
import me.libme.rec.receiver.vo.UserItemRecord;
import me.libme.webboot.ResponseModel;
import me.libme.webseed.web.ClosureException;
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


    @ResponseBody
    @RequestMapping(path="/message",method= RequestMethod.POST)
    public ResponseModel message (Event event, UserItemRecord userItemRecord, Content content) throws Exception {
        // do something validation on the contentRecord.

        return ResponseModel.newSuccess(true);
    }




}
