package me.libme.rec.receiver.web;

import me.libme.cls.cluster.Cluster;
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
import scalalg.me.libme.rec.RecRuntime;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * Created by J on 2018/1/20.
 */
@Component
@Conditional({DefaultStarterComponent._DefaultStarterCondition.class})
public class DefaultStarterComponent implements ApplicationListener<ContextRefreshedEvent> {

    private Logger logger=LoggerFactory.getLogger(DefaultStarterComponent.class);


    public DefaultStarterComponent() {
        logger.info("Construct "+this.getClass());
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStarterComponent.class);

    private static final String SWITCH="cpp.rec.default.starter.switch";

    public static class _DefaultStarterCondition implements Condition {

        private Logger logger=LoggerFactory.getLogger(_DefaultStarterCondition.class);

        public _DefaultStarterCondition() {
            logger.info("Construct "+this.getClass());
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



    private ScheduledExecutorService windowExecutor= Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r,"window-topology-scheduler-receiver");
        }
    });

    private ExecutorService executor=Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
            r->new Thread(r,"real thread on executing topology receiver"));


    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {

        RecProcessor.builder()
                .setArgs(RecApplication.args)
                .setQueueHolder(queueHolder)
                .windowExecutor(windowExecutor)
                .executor(executor)
                .build().start();


        RecRuntime recRuntime=RecRuntime.builder().getOrCreate();
        if(recRuntime.isCluster()){
            Cluster cluster=new Cluster(RecApplication.args);
            try {
                cluster.start();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

    }


}
