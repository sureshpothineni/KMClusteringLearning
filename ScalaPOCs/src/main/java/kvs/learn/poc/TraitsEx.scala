package kvs.learn.poc

/**
 * trait is just like abstract class in java. trait can have some concrete and some prototype methods. 
 * trait wont have constructor parameters.
 */
object TraitsEx {

	def main(args:Array[String]){
		    var obj = new class1()
				obj.display("suresh")
				obj.printx("testing")
	}
}

trait interface1 {
	def display(parm: String)

	def printx(param:String){
		println("this is trait test printing:"+ param);
	}
}

class class1  extends interface1 {

	@Override 
	def display(parm: String){
		println("this is overrided display method");
	}
}