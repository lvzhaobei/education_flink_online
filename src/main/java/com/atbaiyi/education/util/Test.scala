package com.atbaiyi.education.util

import java.math.BigInteger
import java.security.MessageDigest

object Test {
  def main(args: Array[String]): Unit = {
    Animal.test
  }

  /**
    * 对字符串进行MD5加密
    *
    * @param input
    * @return
    */
  def generateHash(input: String): String = {
    try {
      if (input == null) {
        null
      }
      val md = MessageDigest.getInstance("MD5")
      md.update(input.getBytes());
      val digest = md.digest();
      val bi = new BigInteger(1, digest);
      var hashText = bi.toString(16);
      while (hashText.length() < 32) {
        hashText = "0" + hashText;
      }
      hashText
    } catch {
      case e: Exception => e.printStackTrace(); null
    }
  }
}

object Animal {

  def test:Unit = {
    println("11111")
  }
}

class Animal {

  def test:Unit = {
    println("22222")
  }
}
