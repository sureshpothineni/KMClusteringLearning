package kvs.learn.poc
/**
 * this example describes about user overridden currying function behavior
 */
object SomeCurryingEx {
  
def add(a: Int, b: Long): Double = a.toDouble + b
def main(args:Array[String]){
  
	 val spicyAdd = Impl.curry(add)
			println(spicyAdd(1)(2L)) // prints "3.0"
			val increment = spicyAdd(1) // increment holds a function which takes a long and adds 1 to it.
			println(increment(1L)) // prints "2.0"
			val unspicedAdd = Impl.uncurry(spicyAdd)
			println(unspicedAdd(4, 5L)) // prints "9.0"
}
}

// Here's a trait encapsulating the definition 
trait Given {
  def curry[A,B,C](f:(A,B) => C) : A => B => C
  def uncurry[A,B,C](f:A => B => C): (A,B) => C
}

object Impl extends Given {
  // I'm going to implement uncurry first because it's the easier of the
  // two to understand.  The bit in curly braces after the equal sign is a
  // function literal which takes two arguments and applies the to (i.e.
  // uses it as the arguments for) a function which returns a function.
  // It then passes the second argument to the returned function.
  // Finally it returns the value of the second function.
  def uncurry[A,B,C](f:A => B => C): (A,B) => C = { (a: A, b: B) => f(a)(b) }

  // The bit in curly braces after the equal sign is a function literal
  // which takes one argument and returns a new function.  I.e., curry()
  // returns a function which when called returns another function
  def curry[A,B,C](f:(A,B) => C) : A => B => C = { (a: A) => { (b: B) => f(a,b) } }
}

