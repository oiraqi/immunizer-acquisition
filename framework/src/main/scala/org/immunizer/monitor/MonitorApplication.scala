import java.util.StringTokenizer

object MonitorApplication {
    def main(args: Array[String]): Unit = {
        val message = "Hello World"
        val tokenizer = new StringTokenizer(message, " ")   
        println(tokenizer.nextToken())
        println(tokenizer.nextToken())
    }
}