package com.example.demo;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.RetryState;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.policy.ExceptionClassifierRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

@SpringBootApplication
@EnableKafka
@EmbeddedKafka
public class DemoApplication {

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Bean
    public EmbeddedKafkaBroker kafkaBroker() {
        EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(1, true);
        broker.kafkaPorts(9092);
        return broker;
    }

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public NewTopic topic() {
        return new NewTopic("kgh968", 2, (short) 1);
    }

    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, String> template) {
        return args -> {
            IntStream.range(0, 2).forEach(i -> template.send("kgh968", 0, null, String.valueOf(i)));
        };
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        factory.setConcurrency(2);
        factory.getContainerProperties().setAckOnError(false);
        factory.setMessageConverter(new MessagingMessageConverter());
        factory.setStatefulRetry(true);

        // communicate between the retrytemplate and the seeking error handler
        ThreadLocal<Boolean> shouldSeek = ThreadLocal.withInitial(() -> true);
        factory.setErrorHandler(new SeekToCurrentErrorHandler(-1) {
            @Override
            public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records,
                               Consumer<?, ?> consumer, MessageListenerContainer container) {
                if (shouldSeek.get()) {
                    super.handle(thrownException, records, consumer, container);
                }
            }
        });

        RetryTemplate retryTemplate = new RetryTemplate() {
            @Override
            protected boolean shouldRethrow(RetryPolicy retryPolicy, RetryContext context, RetryState state) {
                shouldSeek.set(retryPolicy.canRetry(context));
                return true;
            }
        };


        ExceptionClassifierRetryPolicy policy = new ExceptionClassifierRetryPolicy();
        policy.setExceptionClassifier(
                new UnrwappingSubclassClassifier(
                        new HashMap<Class<? extends Throwable>, RetryPolicy>() {
                            {
                                put(ExceptionA.class, new AlwaysRetryPolicy());
                                put(ExceptionB.class, new SimpleRetryPolicy(5));
                            }
                        },
                        new NeverRetryPolicy()
                ));


        retryTemplate.setRetryPolicy(policy);
        retryTemplate.setBackOffPolicy(new ExponentialBackOffPolicy());

        factory.setRetryTemplate(retryTemplate);

        return factory;
    }

    @KafkaListener(topics = "kgh968",
            groupId = "kgh968",
            properties = {"max.poll.interval.ms:120000"})
    public void listen(String in) {
        log.debug(in);
        System.err.println("----------" + in);
        if (in.equalsIgnoreCase("1"))
            throw new ExceptionA("------> " + in);
        else
            throw new ExceptionB("------> " + in);

    }
}
