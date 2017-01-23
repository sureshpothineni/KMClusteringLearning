package kvs.learn.poc
/**
 * if an function has multiple lists of parameters, if function is called with missing parameter lists,
 *  this function yields another function to get the missed parameters. this is called currying.
 *  currying will be achieved with partial functions.
 */
object currying1 {

	def main(args:Array[String]){
		val call1 = currying(5)_  //partial function call , _ is required, otherwise compilation error.
				call1(6) // passing missed parameter list
		
		    // another example with out _ is used because partial function called as function param
				val nums = List(1, 2, 3, 4, 5, 6, 7, 8)
				println(filter(nums, modN(_)(2))) // passing second param list
				println(filter(nums, modN(3)))
	}

	def currying(x:Int)(y:Int){
		println(s"currying output is :${x+y}")
	}
	def filter(xs: List[Int], p: Int => Boolean): List[Int] ={
			if (xs.isEmpty) xs
			else if (p(xs.head)) xs.head :: filter(xs.tail, p)
			else filter(xs.tail, p)
	}

	def modN(n: Int)(x: Int) = ((x % n) == 0)

	
}