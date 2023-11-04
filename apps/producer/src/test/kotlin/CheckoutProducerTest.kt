import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CheckoutProducerTest {
  @Test
  fun `Should produce 10 messages`() {
    val producer = CheckoutProducer()
    producer.start()
    producer.stop()
  }
}
