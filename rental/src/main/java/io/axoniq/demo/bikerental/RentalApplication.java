package io.axoniq.demo.bikerental;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.axoniq.demo.bikerental.coreapi.payment.PaymentStatus;
import io.axoniq.demo.bikerental.coreapi.rental.BikeStatus;
import io.micrometer.core.instrument.MeterRegistry;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.commandhandling.gateway.IntervalRetryScheduler;
import org.axonframework.config.Configuration;
import org.axonframework.config.EventProcessingConfigurer;
import org.axonframework.deadline.DeadlineManager;
import org.axonframework.deadline.SimpleDeadlineManager;
import org.axonframework.eventhandling.tokenstore.jpa.TokenEntry;
import org.axonframework.eventsourcing.eventstore.jpa.DomainEventEntry;
import org.axonframework.messaging.StreamableMessageSource;
import org.axonframework.modelling.saga.repository.jpa.SagaEntry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

//@EntityScan(basePackageClasses = {PaymentStatus.class, BikeStatus.class, DomainEventEntry.class, SagaEntry.class, TokenEntry.class})
@SpringBootApplication
@EnableScheduling
public class RentalApplication {

    public static void main(String[] args) {
        SpringApplication.run(RentalApplication.class, args);
    }


    //configuring retry scheduler for command gateway.  This will handle retrying if a non-transient exception occurs
    //when attempting to send a command
    //https://docs.axoniq.io/reference-guide/axon-framework/axon-framework-commands/infrastructure#retryscheduler
//    @Bean
//    public CommandGateway commandGatewayWithRetry(CommandBus commandBus){
//
//        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);
//        IntervalRetryScheduler rs = IntervalRetryScheduler.builder().retryExecutor(scheduledExecutorService).maxRetryCount(5).retryInterval(1000).build();
//        return DefaultCommandGateway.builder().commandBus(commandBus).retryScheduler(rs).build();
//    }

    @Bean(destroyMethod = "shutdown")
    public ScheduledExecutorService workerExecutorService() {
        return Executors.newScheduledThreadPool(4);
    }

    @Autowired
    public void configureSerializers(ObjectMapper objectMapper) {
        objectMapper.activateDefaultTyping(objectMapper.getPolymorphicTypeValidator(),
                                           ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT);
    }




    @Autowired
    public void configure(EventProcessingConfigurer eventProcessing) {
        eventProcessing.registerPooledStreamingEventProcessor(
                "PaymentSagaProcessor",
                Configuration::eventStore,
                (c, b) -> b.workerExecutor(workerExecutorService())
                           .batchSize(100)
                           .initialToken(StreamableMessageSource::createHeadToken)
        );
        eventProcessing.registerPooledStreamingEventProcessor(
                "BikeStatus",
                Configuration::eventStore,
                (c, b) -> b.workerExecutor(workerExecutorService())
                           .batchSize(100)

        );
    }

    @Bean
    public DeadlineManager deadlineManager(Configuration configuration) {
        return SimpleDeadlineManager.builder().scopeAwareProvider(configuration.scopeAwareProvider()).build();
    }


    //monitoring configuration
    @Bean
    MeterRegistryCustomizer<MeterRegistry> metricsCommonTags(@Value("${axon.axonserver.enabled}") boolean axonServerEnabled) {
        String appName = "bike-rental";
        if (axonServerEnabled) {
            appName = appName + "-axonserver";
        } else {
            appName = appName + "-postgres";
        }
        String finalAppName = appName;
        return registry -> registry.config().commonTags("app", finalAppName);
    }

}
