package pengyu.order.search;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.test.context.TestPropertySource;
import pengyu.order.config.KafkaErrorHandlerConfig;
import pengyu.order.event.OrderCompletedEventPublisher;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Consumer 실패 시 Retry -> DLT 이동을 로컬 Kafka(docker-compose)로 검증한다.
 * - 재시도 간격은 테스트 시간을 줄이려고 짧게 바꾼다.
 * - 캐시된 다른 테스트 컨텍스트의 Consumer가 같은 그룹에 있으면 파티션을 그쪽이 가져가서
 *   이 컨텍스트(SpyBean, 짧은 재시도)가 메시지를 못 받을 수 있다. 그래서 실행마다 새 그룹을 쓴다.
 */
@SpringBootTest
@TestPropertySource(properties = {
        "kafka.retry.initial-interval-ms=50",
        "kafka.retry.max-retries=3",
        "search.consumer.group-id=order-search-dlt-test-${random.uuid}",
        // 새 그룹이 earliest로 시작하면 토픽 전체를 다시 읽어서, 다른 테스트가 지운 문서까지 ES에 되살려 놓는다
        "spring.kafka.consumer.auto-offset-reset=latest"
})
class OrderCompletedConsumerDltTest {

    private static final int MAX_RETRIES = 3;
    private static final TopicPartition DLT_PARTITION = new TopicPartition(KafkaErrorHandlerConfig.ORDER_COMPLETED_DLT, 0);

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    String bootstrapServers;

    @SpyBean
    OrderSearchRepository orderSearchRepository;

    @Autowired
    KafkaListenerEndpointRegistry listenerRegistry;

    // latest는 "파티션을 할당받은 시점의 끝"부터 읽으므로, 할당 전에 보낸 메시지는 못 받는다. 할당될 때까지 기다린다.
    @BeforeEach
    void waitForPartitionAssignment() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (listenerRegistry.getListenerContainers().stream().anyMatch(this::notAssignedYet)) {
            assertThat(System.currentTimeMillis()).as("Consumer 파티션 할당 대기 시간 초과").isLessThan(deadline);
            Thread.sleep(200);
        }
    }

    private boolean notAssignedYet(MessageListenerContainer container) {
        Collection<TopicPartition> assigned = container.getAssignedPartitions();
        return assigned == null || assigned.isEmpty();
    }

    @Test
    @DisplayName("JSON이 깨진 메시지는 재시도 없이 원본 그대로 DLT로 간다")
    void malformedMessage_goesToDltWithoutRetry() {
        try (KafkaConsumer<String, String> dltConsumer = dltConsumerAtEnd()) {
            String key = "malformed-" + UUID.randomUUID();
            String brokenJson = "{not-a-json";

            send(key, brokenJson);

            ConsumerRecord<String, String> dead = pollDlt(dltConsumer, key);
            assertThat(dead.value()).isEqualTo(brokenJson); // 따옴표로 한 번 더 감싸지지 않고 원본 그대로
            assertThat(new String(dead.headers().lastHeader("kafka_dlt-exception-fqcn").value()))
                    .contains("ListenerExecutionFailedException");
        }
    }

    @Test
    @DisplayName("ES 저장이 계속 실패하면 (1 + 재시도 횟수)만큼 시도한 뒤 DLT로 간다")
    void esFailure_retriesThenGoesToDlt() {
        long orderId = -System.nanoTime(); // 실제 주문과 겹치지 않는 ID
        doThrow(new RuntimeException("ES 장애 재현"))
                .when(orderSearchRepository).save(argThat(d -> d != null && d.getOrderId() == orderId));

        try (KafkaConsumer<String, String> dltConsumer = dltConsumerAtEnd()) {
            String key = String.valueOf(orderId);
            String event = """
                    {"orderId":%d,"menuId":1,"menuName":"양념 치킨","totalPrice":18000,"status":"COMPLETED","createdAt":"2026-10-12T12:00:00"}
                    """.formatted(orderId);

            send(key, event);

            ConsumerRecord<String, String> dead = pollDlt(dltConsumer, key);
            assertThat(dead.value()).isEqualTo(event);
        }
        verify(orderSearchRepository, times(1 + MAX_RETRIES)).save(argThat(d -> d != null && d.getOrderId() == orderId));

        // 다른 테스트 컨텍스트의 Consumer(기본 그룹)는 정상 저장했을 수 있으므로 정리한다
        orderSearchRepository.deleteById(String.valueOf(orderId));
    }

    private void send(String key, String value) {
        Map<String, Object> props = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(OrderCompletedEventPublisher.TOPIC, key, value));
        }
    }

    // 지금 DLT 끝 위치부터 읽어서, 이 테스트가 만든 메시지만 본다
    private KafkaConsumer<String, String> dltConsumerAtEnd() {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.assign(List.of(DLT_PARTITION));
        consumer.seekToEnd(List.of(DLT_PARTITION));
        consumer.position(DLT_PARTITION); // seekToEnd는 lazy라 여기서 위치를 확정시킨다
        return consumer;
    }

    private ConsumerRecord<String, String> pollDlt(KafkaConsumer<String, String> consumer, String key) {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                if (key.equals(record.key())) {
                    return record;
                }
            }
        }
        throw new AssertionError("DLT에서 key=" + key + " 메시지를 찾지 못함");
    }
}
