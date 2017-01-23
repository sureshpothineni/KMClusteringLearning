package kvs.learn.poc

object MixinComposition {
  
 def main(args: Array[String]) {
    var value = " this is testing mixin-composition string iteration"
    class Iter extends StringIterator(value) with RichIterator
    val iter = new Iter
    iter foreach print
  } 
 
}

 abstract class AbsIterator {
  type T
  def hasNext: Boolean
  def next: T
}
 
trait RichIterator extends AbsIterator {
  def foreach(f: T => Unit) { while (hasNext) f(next) }
}

class StringIterator(s: String) extends AbsIterator {
  type T = Char
  private var i = 0
  def hasNext = i < s.length()
  def next = { val ch = s charAt i; i += 1; ch }
}

