//package pickle
//
//import org.scalatest.FunSuite
//
//import scala.pickling._
//import json._
//
//@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
//class RuntimeArrayTest extends FunSuite {
//
//  test("primitive array") {
//    def testIt(x: Any): Unit = {
//      val p = x.pickle
//      val up = p.unpickle[Any]
//      val arr = up.asInstanceOf[Array[Int]]
//      assert(arr.mkString(",") == "5,6")
//    }
//
//    val arr: Array[Int] =
//      Array(5, 6)
//
//    testIt(arr)
//  }
//
//  test("non-primitive array") {
//    def testIt(x: Any): Unit = {
//      val p = x.pickle
//      val up = p.unpickle[Any]
//      val arr = up.asInstanceOf[Array[(Int, Double)]]
//      assert(arr.mkString(",") == "(5,0.5),(6,0.6)")
//    }
//
//    val arr: Array[(Int, Double)] =
//      Array(5 -> 0.5d, 6 -> 0.6d)
//
//    testIt(arr)
//  }
//
//  test("non-primitive list") {
//    def testIt(x: Any): Unit = {
//      val p = x.pickle
//      val up = p.unpickle[Any]
//      val arr = up.asInstanceOf[List[(Int, Double)]]
//      assert(arr.mkString(",") == "(5,0.5),(6,0.6)")
//    }
//
//    val list: List[(Int, Double)] =
//      List(5 -> 0.5d, 6 -> 0.6d)
//
//    //testIt(list) // FIXME
//  }
//
//}
