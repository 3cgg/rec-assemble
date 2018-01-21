package test.me.libme.rec;

import me.libme.kernel._c.http.JHttp;
import me.libme.kernel._c.util.JDateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by J on 2018/1/21.
 */
public class TestJavaTrackData {

    private static Logger logger= LoggerFactory.getLogger(TestJavaTrackData.class);

    private static long count;

    public static void main(String[] args) throws Exception {

        logger.debug("-------debug--------");
        logger.info("---------info------");

        String content= String.valueOf(JHttp._get().execute("http://www.baidu.com"));

        System.out.println(content);

        Random random=new Random(99);
        Random random1=new Random(8);
        Random r=new Random(6);


        Executors.newScheduledThreadPool(1)
                .scheduleAtFixedRate(()->{
                    try {
                        for(int i=0;i<5000;i++){
                            count++;
                            JHttp._post()
                                    .putParam("source","app")
                                    .putParam("time", JDateUtils.formatWithSeconds(new Date()))
                                    .putParam("type",(i%3==0?"click":"browser"))

                                    .putParam("userId",random.nextInt(10000))
                                    .putParam("itemId",random1.nextInt(10000))

                                    .putParam("desc","测试目的_"+r.nextInt())
                                    .putParam("data","("+count+"th)数据放这里_"+r.nextInt())
//                                    .setProxy("127.0.0.1",8888)
                                    .putHead("Content-Type","application/x-www-form-urlencoded;charset=UTF-8")
                                    .execute("http://localhost:8989/api/receiver/message");

                            logger.info("send the "+count+"th record" );
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                },0,5000, TimeUnit.MILLISECONDS);





    }

}
