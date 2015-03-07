package org.sparklinedata.reflection

object ReflectionUtils {

  import scala.reflect.runtime.{universe => ru}

  def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]

  def getType(o : Any) : ru.Type = {
    val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
    val clsSymbol = runtimeMirror.classSymbol(o.getClass)
    clsSymbol.toType
  }

  def intType = ru.typeOf[Int]
  def integerType = ru.typeOf[java.lang.Integer]

  def primitiveMatch(t1 : ru.Type, t2 : ru.Type) : Boolean = (t1, t2) match {
    case (intType, integerType) => true
    case _ => false
  }

  def construct[T: ru.TypeTag](args : Any*) : T = {
    val typ = ru.typeOf[T]
    val classSymbol = typ.typeSymbol.asClass
    val runtimeMirror = ru.runtimeMirror(getClass.getClassLoader)
    val classMirror = runtimeMirror.reflectClass(classSymbol)
    val ctors = typ.decl(ru.termNames.CONSTRUCTOR).asTerm
    val argTypes = args.map(getType(_))

    ctors.alternatives.foreach { c =>
      val ctor = c.asMethod
      val paramss = ctor.paramLists
      if (paramss.size == 1) {
        val params = paramss(0)

        var noMatch = true
        if (params.size == argTypes.size) {
          noMatch = false
          (params zip argTypes).foreach { t =>
            val p = t._1
            val argT = t._2
            val nm = p.name
            val pTyp = p.typeSignature
            if ( !((pTyp weak_<:< argT) /*|| primitiveMatch(pTyp, argT)*/) ) {
              noMatch = true
            }
          }
        }
        if (!noMatch ) {
          val constructorMirror = classMirror.reflectConstructor(ctor)
          return constructorMirror(args:_*).asInstanceOf[T]
        }
      }
    }
    throw new NoSuchMethodException(s"${classSymbol.name}:constructor}")
  }

}
