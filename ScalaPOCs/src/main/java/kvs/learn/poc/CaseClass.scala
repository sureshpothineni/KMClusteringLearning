package kvs.learn.poc
/**
 * case classes will define implicit setters(if param is var only), getters and primary constructor.
 * user can achieve immutable classes by using case classes provided no class arguments are var type.
 * comparing case classes with help of == you check that each value inside of given classes.
 * Companion classes:
 * if user would like to write some logic( methods, operations), scala suggest to write those in companion classes
 *   than writing in the case class. user can also write those in case classes, but it is not suggestable.
 * In order to define a companion object for a class you have to set the same names for them 
 *   and declare them in the same file.companion classes are used to provide Static variables/functions
 */
case class CaseClass(val id:Int, var name:String);

object CaseClass{
  
  private var staticVal = 0;
  
  def display( obj:CaseClass){
    println(s"Given Object values Id: ${obj.id.toString()} and name: ${obj.name}")
  }
  def increment(){ 
    staticVal += 1;
    println("display static value after increment: "+staticVal);
  }
  
  def main(args:Array[String]){
    var obj1 = new CaseClass(5,"Suresh");
    CaseClass.increment();
    CaseClass.display(obj1);
    println("Object 1: "+obj1.id +","+obj1.name);
    //obj1.name = "Naveen" // user can change value of case class (not immutable)
    var obj2 = new CaseClass(5,"Suresh");
    CaseClass.increment();
    println("Object 1: "+obj2.productElement(0) +","+obj2.productElement(1)); 
    if(obj1.eq(obj2)){
      println("eq is True :");//false
    }
    if(obj1.equals(obj2)){
      println("Equals is true"); //true
    }
    if(obj1 == obj2){
      println("== is true"); //true
    }
    //val tupledPerson: ((Int, String, Double)) => CaseClassInDepth = CaseClassInDepth.tupled
  }
}