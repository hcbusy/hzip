package org.yagnus.hzip.io
import org.yagnus.yadoop.io.IOAImpl
import org.yagnus.yadoop.io.Versioned

trait HZIO[T <: AnyRef with Versioned] extends IOAImpl[T] with HZVer {
  this: T =>
}