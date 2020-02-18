import java.util.StringTokenizer

object AcquisitionApplication {
    def main(args: Array[String]): Unit = {
        val message = "Hello World"
        val tokenizer = new StringTokenizer(message, " ")   
        println(tokenizer.nextToken())
        println(tokenizer.nextToken())
    }
}