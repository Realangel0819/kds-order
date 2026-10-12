package pengyu.order.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import pengyu.order.event.OrderCompletedEventPublisher;

import java.util.Map;

/**
 * Kafka Consumer 실패 처리: 간격을 늘려가며 재시도(Retry) -> 그래도 실패하면 DLT(Dead Letter Topic)로 보관.
 * Boot가 CommonErrorHandler 빈을 찾아 @KafkaListener 컨테이너에 자동으로 붙여준다.
 */
@Slf4j
@Configuration
public class KafkaErrorHandlerConfig implements DisposableBean {

    // DeadLetterPublishingRecoverer 기본 규칙: 원본 토픽 + ".DLT", 원본과 같은 파티션 번호로 보낸다
    public static final String ORDER_COMPLETED_DLT = OrderCompletedEventPublisher.TOPIC + ".DLT";

    private DefaultKafkaProducerFactory<String, String> dltProducerFactory;

    // 원본/DLT 파티션 수를 맞춰 둔다 (DLT 파티션이 더 적으면 "같은 파티션 번호"로 보낼 수 없음)
    @Bean
    public NewTopic orderCompletedTopic() {
        return TopicBuilder.name(OrderCompletedEventPublisher.TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic orderCompletedDltTopic() {
        return TopicBuilder.name(ORDER_COMPLETED_DLT).partitions(1).replicas(1).build();
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaProperties kafkaProperties,
                                                 @Value("${kafka.retry.initial-interval-ms:1000}") long initialIntervalMs,
                                                 @Value("${kafka.retry.max-retries:4}") int maxRetries) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dltTemplate(kafkaProperties));

        // 1초 -> 2초 -> 4초 -> 8초 (최대 10초). 기본값(간격 0으로 9번)은 ES가 몇 초만 흔들려도 순식간에 소진된다.
        // 주의: 재시도 대기 동안 이 파티션의 다음 메시지도 같이 멈춘다 (총 대기가 max.poll.interval.ms(5분)보다 짧아야 함)
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, e) -> {
            log.error("[Kafka DLT] 재처리 실패로 DLT 이동 - topic={}, key={}, cause={}",
                    record.topic(), record.key(), e.getMessage());
            recoverer.accept(record, e);
        }, backOff);
        // 깨진 메시지는 몇 번을 다시 읽어도 실패하므로 재시도 없이 바로 DLT로 보낸다
        errorHandler.addNotRetryableExceptions(JsonProcessingException.class);
        return errorHandler;
    }

    // Consumer는 값을 String으로 받는데, 기본 KafkaTemplate은 JsonSerializer라 String을 한 번 더 감싸서("\"{...}\"") 보낸다.
    // 원본 메시지를 그대로 DLT에 남기려고 StringSerializer 전용 템플릿을 따로 만든다.
    // (빈으로 등록하면 Boot의 기본 KafkaTemplate 자동 설정이 꺼지므로 내부에서만 쓴다)
    private KafkaTemplate<String, String> dltTemplate(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties(null);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        dltProducerFactory = new DefaultKafkaProducerFactory<>(props);
        return new KafkaTemplate<>(dltProducerFactory);
    }

    @Override
    public void destroy() {
        if (dltProducerFactory != null) {
            dltProducerFactory.destroy();
        }
    }
}
