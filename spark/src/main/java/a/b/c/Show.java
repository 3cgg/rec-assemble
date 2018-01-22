package a.b.c;

import me.libme.kernel._c.json.JJSON;
import s.a.b.c.B;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by J on 2017/12/29.
 */
public class Show {


    public static void main(String[] args) {

        B b=new B("J",20);
        String sss=b.showName();
        System.out.println(sss);

        Map map=new HashMap();
        map.put("A","A-NAME");
        map.put("B","B-NAME");

        System.out.println(JJSON.get().format(map));


    }


}
