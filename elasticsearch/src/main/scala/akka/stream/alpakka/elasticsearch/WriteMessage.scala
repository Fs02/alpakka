/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch

import akka.NotUsed
import akka.annotation.InternalApi
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest

import scala.compat.java8.OptionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] sealed abstract class Operation(val command: String)

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] object Operation {
  object Index extends Operation("index")
  object Update extends Operation("update")
  object Upsert extends Operation("update")
  object Delete extends Operation("delete")
}

final class WriteMessage[T, PT] private (val operation: Operation,
                                         val source: Option[T],
                                         val request: DocWriteRequest[_],
                                         val passThrough: PT = NotUsed) {

  def withSource(value: T): WriteMessage[T, PT] = copy(source = Option(value))

  def withPassThrough[PT2](value: PT2): WriteMessage[T, PT2] =
    new WriteMessage[T, PT2](operation, source, request, value)

  private def copy(operation: Operation = operation,
                   source: Option[T] = source,
                   request: DocWriteRequest[_] = request,
                   passThrough: PT = passThrough): WriteMessage[T, PT] =
    new WriteMessage[T, PT](operation, source, request, passThrough)

  override def toString =
    s"""WriteMessage(operation=$operation,source=$source,request=$request,passThrough=$passThrough)"""

  override def equals(other: Any): Boolean = other match {
    case that: WriteMessage[_, _] =>
      java.util.Objects.equals(this.operation, that.operation) &&
      java.util.Objects.equals(this.source, that.source) &&
      java.util.Objects.equals(this.request, that.request) &&
      java.util.Objects.equals(this.passThrough, that.passThrough)
    case _ => false
  }

  override def hashCode(): Int =
    passThrough match {
      case pt: AnyRef =>
        java.util.Objects.hash(operation, source, request, pt)
      case _ =>
        java.util.Objects.hash(operation, source, request)
    }
}

object WriteMessage {
  import Operation._

  def createIndexMessage[T](source: T): WriteMessage[T, NotUsed] =
    new WriteMessage(Index, source = Option(source), request = new IndexRequest())

//  def createIndexMessage[T](source: T, request: IndexRequest): WriteMessage[T, NotUsed] =
//    new WriteMessage(Index, source = Option(source), request = request)

  def createIndexMessage[T](id: String, source: T): WriteMessage[T, NotUsed] =
    createIndexMessage(id, source, new IndexRequest())

  def createIndexMessage[T](id: String, source: T, request: IndexRequest): WriteMessage[T, NotUsed] = {
    request.id(id)
    new WriteMessage(Index, source = Option(source), request = request)
  }

  def createUpdateMessage[T](id: String, source: T): WriteMessage[T, NotUsed] =
    createUpdateMessage(id, source, new UpdateRequest())

  def createUpdateMessage[T](id: String, source: T, request: UpdateRequest): WriteMessage[T, NotUsed] = {
    request.id(id)
    new WriteMessage(Update, source = Option(source), request = request)
  }

  def createUpsertMessage[T](id: String, source: T): WriteMessage[T, NotUsed] =
    createUpsertMessage(id, source, new UpdateRequest())

  def createUpsertMessage[T](id: String, source: T, request: UpdateRequest): WriteMessage[T, NotUsed] = {
    request.id(id)
    request.docAsUpsert(true)
    new WriteMessage(Upsert, source = Option(source), request = request)
  }

  def createDeleteMessage[T](id: String): WriteMessage[T, NotUsed] =
    createDeleteMessage(id, new DeleteRequest())

  def createDeleteMessage[T](id: String, request: DeleteRequest): WriteMessage[T, NotUsed] = {
    request.id(id)
    new WriteMessage(Delete, source = None, request = request)
  }

}

/**
 * Stream element type emitted by Elasticsearch flows.
 *
 * The constructor is INTERNAL API, but you may construct instances for testing by using
 * [[akka.stream.alpakka.elasticsearch.testkit.MessageFactory]].
 */
final class WriteResult[T2, C2] @InternalApi private[elasticsearch] (val message: WriteMessage[T2, C2],
                                                                     val error: Option[Exception]) {
  val success: Boolean = error.isEmpty

  /** Java API */
  def getError: java.util.Optional[Exception] = error.asJava

  override def toString =
    s"""WriteResult(message=$message,error=${error.toString})"""

  override def equals(other: Any): Boolean = other match {
    case that: WriteResult[T2, C2] =>
      java.util.Objects.equals(this.message, that.message) &&
      java.util.Objects.equals(this.error, that.error)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(message, error)
}

trait MessageWriter[T] {
  def convert(message: T): String
}
