import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.After
import org.junit.Before
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName
import java.util.Properties

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CheckoutProducerTest {
  companion object {
    @JvmStatic
    @Container
    val kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.1"))
  }

  @Before
  fun before() {
    kafkaContainer.start()
  }

  @After
  fun after() {
    kafkaContainer.stop()
  }

  @Test
  fun `Should produce messages`() {
    kafkaContainer.start()
    val config = CheckoutProducerConfig(
      years = 1,
      numBonuskaartIds = 1,
      adminProps = Properties().let {
        it[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.bootstrapServers
        it
      },
      producerProps = Properties().let {
        it[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.bootstrapServers
        it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
        it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = SpecificAvroSerializer::class.java
        it["schema.registry.url"] = "mock://schema-registry"
        it
      },
    )
    val producer = CheckoutProducer(config)
    // TODO use testcontainers
    // TODO clean topic etc.
    producer.start()
    producer.stop()
  }
}
