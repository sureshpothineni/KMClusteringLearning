package kvs.learn.poc

/**
 * when user defines case class 
 * it Creates a class and its companion object.
 * 2. Implements the apply method that you can use as a factory. This lets you create instances of the class without the new keyword
 * 3. Prefixes all arguments, in the parameter list with val. This means the class is immutable
 * 4. Adds natural implementations of hashCode, equals and toString.
 *     Since == in Scala always delegates to equals, this means that case class instances are always compared structurally.
 * 5.Generates a copy method to your class to create other instances starting from another one and keeping some arguments the same.
 * 6. since the compiler implements the unapply method, a case class supports pattern matching.
 * 7. when your class has no argument you use a case object instead of a case class with an empty parameter list.
 * 8. unapply method lets you deconstruct a case class to extract it’s fields,
 *     both during pattern matching and as a simple expression to extract some of its fields
 * 9. if you need a function that, given your case class arguments as parameters, creates an instance of the class?
 *     Here’s how you can do it by partially applying apply    
 * 10. since each case class extends the Product trait, it inherits methods(productArity,productElement,productIterator,productPrefix)
 * 11. companion object extends AbstractFunctionN,where N is the number of formal parameters of the case class.
 *         Plus, it will inherit curried and tupled iff N > 1.curried and tupled methods are inherited from AbstractFunctionN
 *  Curried, toupled methods will be available,if case class doesn't have user defined companion object.
 */
case class CaseClassInDepth(id:Int, name:String,marks:Double)

object CaseClassInDepth {
  def main(args:Array[String]){
    val cObj = CaseClassInDepth(1,"Suresh",123.0)  //2.
    println(cObj.hashCode()+":" + cObj.toString()) //4.
    val cObj1 = cObj.copy(name="Praveen")   //5.
    println(cObj1.toString());
    //6. will be covered in matching example
    
    val CaseClassInDepth(_,name,_) =cObj  //8.
    println(s"element name : ${name}");
    
    val caseFun: (Int, String, Double) => CaseClassInDepth = CaseClassInDepth.apply _  //9.
    println("partial apply function:"+caseFun(10,"Nani",123))
    
    //val tupledPerson: ((Int, String, Double)) => CaseClassInDepth = CaseClassInDepth.tupled
    /* this will be covered as part of currying
    val curriedFun: String => String => Double => CaseClassInDepth = CaseClassInDepth.curried
    val lacavaBuilder: String => Int => Person = curriedPerson("Lacava")
    val me = lacavaBuilder("Alessandro")(1976)
    val myBrother = lacavaBuilder("Michele")(1972)*/
    
  }
}