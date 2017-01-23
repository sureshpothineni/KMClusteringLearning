package kvs.learn.poc
/**
 * Higher order function are functions which takes function as parameter or returns function as return value.
 * function that is passed as parameter is called call back function.
 * inc:Int =>Int means, the functin inc takes int argument and returns Int.
 *   i.e. inc[Int,Int] i.e. def inc(x:Int)=>Int ={return x+10}
 *   () => T is the type of a function that takes no arguments and returns a T. It is equivalent to Function0[T].
 */
object Highfun1 {
  
  def main(args:Array[String]){
    //calling higher order function
    highFun1(100,increment)
    highFun2(10,20,add)
  }
  
  // defining higher order function
  def highFun1(amount:Int, inc:Int =>Int){  
    println(s"entered amount:${amount} and incremented amount: ${inc(amount)}");
  }
  
  // defining higher order function with multiple params
  def highFun2(a:Int,b:Int, addFun:(Int,Int) =>Int){
    println(s"entered amounts:${a},${b} and added amount: ${addFun(a,b)}");
  }
  
  //defining call-back function
  def increment(amount:Int):Int ={
    println("incrementing the amount to +10");
    amount +10
  }
  //defining call-back function that takes multiple params
  
  def add(a:Int,b:Int):Int =	  a+b  // returning int.
 
}