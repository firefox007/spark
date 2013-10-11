package spark.util

import org.apache.hadoop.io.Writable
import java.io._
import org.apache.hadoop.security.token.{TokenIdentifier, Token}
import java.util

object TokenDeSerUtils {

  private val SEP: String = ","

  private def _serialize(token: Token[_ <: TokenIdentifier]) : Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val dataOut = new DataOutputStream(out)
    token.write(dataOut)
    dataOut.close()
    out.toByteArray
  }

  private def _deserialize(content: Array[Byte]) : Token[_ <: TokenIdentifier] = {
    val token = new Token()
    val dataIn = new DataInputStream(new ByteArrayInputStream(content))
    token.readFields(dataIn)
    dataIn.close
    token
  }

  private def StringToByteArray(s: String): Array[Byte] = {
    val byteStrings: Array[String] = s.split(SEP)
    val result: Array[Byte] = for(bs <- byteStrings) yield Integer.valueOf(bs).byteValue()
    result
  }

  private def ByteArrayToString(ba: Array[Byte]) : String = {
    val intArray: Array[Int] = for(b <- ba) yield b.toInt
    intArray.mkString(SEP)
  }

  def serialize(token: Token[_ <: TokenIdentifier]) : String = {
    val byteArray =  _serialize(token)
    ByteArrayToString(byteArray)
  }

  def deserialize(content: String) : Token[_ <: TokenIdentifier] = {
    val byteArray = StringToByteArray(content)
    _deserialize(byteArray)
  }

}
