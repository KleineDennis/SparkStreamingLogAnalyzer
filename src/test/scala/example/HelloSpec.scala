package example

import org.scalatest._

class HelloSpec extends FlatSpec with Matchers {
  "The Hello object" should "say Hello World" in {
    Hello.greeting shouldEqual "Hello World"
  }
}
