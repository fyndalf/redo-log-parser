package cli

import org.scalatest._

class LogExtractorSpec extends FlatSpec with Matchers {
  "The LogExtractor object" should "exist" in {
    Main.toString equals "LogExtractor"
  }
}
