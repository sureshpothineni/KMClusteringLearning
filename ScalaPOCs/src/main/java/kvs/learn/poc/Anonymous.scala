package kvs.learn.poc
/**
 * anonymous function is a function that does not have function name.
 *  these will be used, when this function is going to uses only once.
 */
object Anonymous {
  
  def main(args:Array[String]){
    //defining anonymous function
    var inc = (x:Int, y:Int)=> x+y
    var x =20
    val y = 30
    println(s" anonymous function return value for given values ${x}, ${y} is ${inc(x,y)}");
  }
  
}