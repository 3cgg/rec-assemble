package me.libme.rec.receiver.web;

import me.libme.rec.RecApplication;
import me.libme.rec.receiver.QueueHolder;
import me.libme.rec.receiver.RecProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.stereotype.Component;

/**
 * Created by J on 2018/1/20.
 */
@Component
@Conditional({DefaultStarterComponent._DefaultStarterCondition.class})
public class DefaultStarterComponent implements ApplicationListener<ContextRefreshedEvent> {


    public DefaultStarterComponent() {
        System.out.println("Construct "+this.getClass());
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStarterComponent.class);

    private static final String SWITCH="cpp.rec.default.starter.switch";

    public static class _DefaultStarterCondition implements Condition {

        public _DefaultStarterCondition() {
            System.out.println("Construct "+this.getClass());
        }

        @Override
        public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
            Environment environment= context.getEnvironment();
            LOGGER.info(SWITCH+" : "+environment.getProperty(SWITCH));
            return environment.getProperty(SWITCH, Boolean.class,true);
        }

    }

    @Autowired
    private QueueHolder queueHolder;


    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {

        RecProcessor.builder()
                .setArgs(RecApplication.args)
                .setQueueHolder(queueHolder)
                .build().start();

    }


}
