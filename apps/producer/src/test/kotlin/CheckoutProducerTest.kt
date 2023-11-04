import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CheckoutProducerTest {
  @Test
  fun `Should produce messages`() {
    val config = CheckoutProducerConfig(
      years = 1,
      numBonuskaartIds = 1
    )
    val producer = CheckoutProducer(config)
    // TODO use testcontainers
    // TODO clean topic etc.
    producer.start()
    producer.stop()
  }
}
