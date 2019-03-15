package com.example.demo;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.classify.SubclassClassifier;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.AlwaysRetryPolicy;
import org.springframework.retry.policy.ExceptionClassifierRetryPolicy;
import org.springframework.retry.policy.NeverRetryPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Bean
    public EmbeddedKafkaBroker kafkaBroker() {
        EmbeddedKafkaBroker broker = new EmbeddedKafkaBroker(1, false);
        broker.kafkaPorts(9092);
        return broker;
    }

    @Bean
    public NewTopic topic() {
        return new NewTopic("demo", 3, (short) 1);
    }

    @Bean
    public NewTopic dlTopic() {
        return new NewTopic("demo.DLT", 3, (short) 1);
    }

    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, String> template) {
        return args -> IntStream.range(0, 3).forEach(i -> template.send("demo", i, null, String.valueOf(i)));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory, KafkaTemplate<Object, Object> dlTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(2);

        factory.getContainerProperties().setAckOnError(false);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);

        factory.setMessageConverter(new MessagingMessageConverter());
        factory.setStatefulRetry(true);

        // error handler
        factory.setErrorHandler(new SeekToCurrentErrorHandler(-1) {
            @Override
            public void handle(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer container) {
                Throwable cause = thrownException;
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                System.err.println("ex --> " + cause.getClass().getSimpleName());
                super.handle(thrownException, records, consumer, container);
            }
        });

        RetryTemplate retryTemplate = new RetryTemplate();

        // retry policy
        ExceptionClassifierRetryPolicy policy = new ExceptionClassifierRetryPolicy();
        Map<Class<? extends Throwable>, RetryPolicy> throwableRetryPolicyMap = new HashMap<>();
        // A is always retried
        throwableRetryPolicyMap.put(ExceptionA.class, new AlwaysRetryPolicy());
        // B is retried 5 times
        throwableRetryPolicyMap.put(ExceptionB.class, new SimpleRetryPolicy(5));
        // the rest is never retied
        policy.setExceptionClassifier(new UnrwappingSubclassClassifier(throwableRetryPolicyMap, new NeverRetryPolicy()));

        retryTemplate.setRetryPolicy(policy);
        retryTemplate.setBackOffPolicy(new ExponentialBackOffPolicy());
        factory.setRetryTemplate(retryTemplate);

        factory.setRecoveryCallback(context -> {
            
            Object o = context.getAttribute("record");
            if (o instanceof ConsumerRecord) {
                ConsumerRecord r = (ConsumerRecord) context.getAttribute("record");

                DeadLetterPublishingRecoverer dr = new DeadLetterPublishingRecoverer(dlTemplate);
                dr.accept((ConsumerRecord) o, new Exception(context.getLastThrowable()));

                if (r != null)
                    System.err.println("Sending msg to dead letter topic " + r.toString());
            }
            return null;
        });

        return factory;
    }


    @KafkaListener(topics = "demo.DLT",
            groupId = "demo-dlt",
            properties = {"max.poll.interval.ms:120000"})
    public void dltListener(String in) {
        System.err.println("  DTL message ---->  " + in);
    }

    @KafkaListener(topics = "demo",
            groupId = "demo",
            properties = {"max.poll.interval.ms:120000"})
    public void listen(String in) {
        System.out.println("New incoming message: " + in);
        if ("0".equals(in)) {
            throw new ExceptionA();
        } else if ("1".equals(in)) {
            throw new ExceptionB();
        } else if ("2".equals(in)) {
            throw new ExceptionC();
        }
    }

    static Class<? extends Throwable> getCause(Throwable t, Class<? extends Throwable> defaultCause, Class<? extends Throwable>... causes) {
        List<Class<? extends Throwable>> classifiers = Arrays.asList(causes);
        Throwable cause = t;
        while (cause != null) {
            if (classifiers.contains(cause.getClass())) {
                return cause.getClass();
            }
            cause = cause.getCause();
        }
        return defaultCause;
    }

    static class UnrwappingSubclassClassifier extends SubclassClassifier<Throwable, RetryPolicy> {

        public UnrwappingSubclassClassifier(Map<Class<? extends Throwable>, RetryPolicy> typeMap, RetryPolicy defaultValue) {
            super(typeMap, defaultValue);
        }

        @Override
        public RetryPolicy classify(Throwable classifiable) {
            Throwable cause = classifiable;
            while (cause != null) {
                if (getClassified().containsKey(cause.getClass())) {
                    return super.classify(cause);
                }
                cause = cause.getCause();
            }
            return getDefault();
        }
    }

    static class ExceptionA extends RuntimeException {
    }

    static class ExceptionB extends RuntimeException {
    }

    static class ExceptionC extends RuntimeException {
    }

}