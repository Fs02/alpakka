/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.elasticsearch.Operation._
import akka.stream.alpakka.elasticsearch._
import akka.stream.alpakka.elasticsearch.impl.ElasticsearchFlowStage._
import akka.stream.stage._
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.lucene.uid.Versions
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.VersionType

import scala.collection.mutable
import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] final class ElasticsearchFlowStage[T, C](
    indexName: String,
    typeName: String,
    client: RestHighLevelClient,
    settings: ElasticsearchWriteSettings,
    writer: MessageWriter[T]
) extends GraphStage[FlowShape[WriteMessage[T, C], Future[Seq[WriteResult[T, C]]]]] {

  private val in = Inlet[WriteMessage[T, C]]("messages")
  private val out = Outlet[Future[Seq[WriteResult[T, C]]]]("result")
  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler with StageLogging {

      private var state: State = Idle
      private val queue = new mutable.Queue[WriteMessage[T, C]]()
      private val failureHandler = getAsyncCallback[(Seq[WriteMessage[T, C]], Exception)](handleFailure)
      private val responseHandler = getAsyncCallback[(Seq[WriteMessage[T, C]], BulkResponse)](handleResponse)
      private var failedMessages: Seq[WriteMessage[T, C]] = Nil
      private var retryCount: Int = 0

      override def preStart(): Unit =
        pull(in)

      private def tryPull(): Unit =
        if (queue.size < settings.bufferSize && !isClosed(in) && !hasBeenPulled(in)) {
          pull(in)
        }

      override def onTimer(timerKey: Any): Unit = {
        sendBulkUpdateRequest(failedMessages)
        failedMessages = Nil
      }

      private def handleFailure(args: (Seq[WriteMessage[T, C]], Exception)): Unit = {
        val (messages, exception) = args
        if (!settings.retryLogic.shouldRetry(retryCount, List(exception))) {
          log.error("Received error from elastic. Giving up after {} tries. {}, Error: {}",
                    retryCount,
                    settings.retryLogic,
                    exception)
          failStage(exception)
        } else {
          log.warning("Received error from elastic. Try number {}. {}, Error: {}",
                      retryCount,
                      settings.retryLogic,
                      exception)
          retryCount = retryCount + 1
          failedMessages = messages
          scheduleOnce(RetrySend, settings.retryLogic.nextRetry(retryCount))
        }
      }

      private def handleSuccess(): Unit =
        completeStage()

      private def handleResponse(args: (Seq[WriteMessage[T, C]], BulkResponse)): Unit = {
        val (messages, response) = args

        // If some commands in bulk request failed, pass failed messages to follows.
        val items = response.getItems // responseJson.asJsObject.fields("items").asInstanceOf[JsArray]
        val messageResults: Seq[WriteResult[T, C]] = items.zip(messages).map {
          case (item, message) =>
            val error =
              if (item.isFailed)
                Some(item.getFailure.getCause)
              else
                None

            new WriteResult(message, error)
        }

        val failedMsgs = messageResults.filterNot(_.error.isEmpty)

        if (failedMsgs.nonEmpty && settings.retryLogic.shouldRetry(retryCount, failedMsgs.map(_.error.get).toList)) {
          retryPartialFailedMessages(messageResults, failedMsgs)
        } else {
          forwardAllResults(messageResults)
        }
      }

      private def retryPartialFailedMessages(
          messageResults: Seq[WriteResult[T, C]],
          failedMsgs: Seq[WriteResult[T, C]]
      ): Unit = {
        // Retry partial failed messages
        // NOTE: When we partially return message like this, message will arrive out of order downstream
        // and it can break commit-logic when using Kafka
        retryCount = retryCount + 1
        failedMessages = failedMsgs.map(_.message) // These are the messages we're going to retry
        scheduleOnce(RetrySend, settings.retryLogic.nextRetry(retryCount))

        val successMsgs = messageResults.filter(_.error.isEmpty)
        if (successMsgs.nonEmpty) {
          // push the messages that DID succeed
          emit(out, Future.successful(successMsgs))
        }
      }

      private def forwardAllResults(messageResults: Seq[WriteResult[T, C]]): Unit = {
        retryCount = 0 // Clear retryCount

        // Push result
        emit(out, Future.successful(messageResults))

        // Fetch next messages from queue and send them
        val nextMessages = (1 to settings.bufferSize).flatMap { _ =>
          queue.dequeueFirst(_ => true)
        }

        if (nextMessages.isEmpty) {
          state match {
            case Finished => handleSuccess()
            case _ => state = Idle
          }
        } else {
          sendBulkUpdateRequest(nextMessages)
        }
      }

      private def sendBulkUpdateRequest(messages: Seq[WriteMessage[T, C]]): Unit = {
        val versionType = VersionType.fromString(settings.versionType.getOrElse("internal"))
        val request = new BulkRequest()

        messages.foreach { message =>
          val indexNameToUse: String = message.indexName.getOrElse(indexName)

          val req = message.operation match {
            case Index =>
              new IndexRequest(indexNameToUse, typeName, message.id.orNull)
                .source(writer.convert(message.source.get), XContentType.JSON)
                .version(message.version.getOrElse(Versions.MATCH_ANY))
                .versionType(versionType)
            case Update | Upsert =>
              new UpdateRequest(indexNameToUse, typeName, message.id.get)
                .doc(writer.convert(message.source.get), XContentType.JSON)
                .docAsUpsert(message.operation == Upsert)
                .version(message.version.getOrElse(Versions.MATCH_ANY))
                .versionType(versionType)
            case Delete =>
              new DeleteRequest(indexNameToUse, typeName, message.id.get)
                .version(message.version.getOrElse(Versions.MATCH_ANY))
                .versionType(versionType)
          }

          request.add(req)
        }

        log.debug("Posting data to Elasticsearch: {}", request.toString)

        client.bulkAsync(
          request,
          new ActionListener[BulkResponse] {
            override def onResponse(response: BulkResponse): Unit =
              responseHandler.invoke((messages, response))
            override def onFailure(exception: Exception): Unit =
              failureHandler.invoke((messages, exception))
          }
        )
//        val json = messages
//          .map { message =>
//            val indexNameToUse: String = message.indexName.getOrElse(indexName)
//            val additionalMetadata = message.customMetadata.map { case (field, value) => field -> JsString(value) }
//
//            JsObject(message.operation match {
//              case Index =>
//                "index" -> JsObject(
//                  (Seq(
//                    Option("_index" -> JsString(indexNameToUse)),
//                    Option("_type" -> JsString(typeName)),
//                    message.version.map { version =>
//                      "_version" -> JsNumber(version)
//                    },
//                    settings.versionType.map { versionType =>
//                      "version_type" -> JsString(versionType)
//                    },
//                    message.id.map { id =>
//                      "_id" -> JsString(id)
//                    }
//                  ).flatten ++ additionalMetadata): _*
//                )
//              case Update | Upsert =>
//                "update" -> JsObject(
//                  (Seq(
//                    Option("_index" -> JsString(indexNameToUse)),
//                    Option("_type" -> JsString(typeName)),
//                    message.version.map { version =>
//                      "_version" -> JsNumber(version)
//                    },
//                    settings.versionType.map { versionType =>
//                      "version_type" -> JsString(versionType)
//                    },
//                    Option("_id" -> JsString(message.id.get))
//                  ).flatten ++ additionalMetadata): _*
//                )
//              case Delete =>
//                "delete" -> JsObject(
//                  (Seq(
//                    Option("_index" -> JsString(indexNameToUse)),
//                    Option("_type" -> JsString(typeName)),
//                    message.version.map { version =>
//                      "_version" -> JsNumber(version)
//                    },
//                    settings.versionType.map { versionType =>
//                      "version_type" -> JsString(versionType)
//                    },
//                    Option("_id" -> JsString(message.id.get))
//                  ).flatten ++ additionalMetadata): _*
//                )
//            }).toString + messageToJsonString(message)
//          }
//          .mkString("", "\n", "\n")

//        log.debug("Posting data to Elasticsearch: {}", json)

//        client.performRequestAsync(
//          "POST",
//          "/_bulk",
//          java.util.Collections.emptyMap[String, String](),
//          new StringEntity(json, StandardCharsets.UTF_8),
//          new ResponseListener() {
//            override def onFailure(exception: Exception): Unit =
//              failureHandler.invoke((messages, exception))
//            override def onSuccess(response: Response): Unit =
//              responseHandler.invoke((messages, response))
//          },
//          new BasicHeader("Content-Type", "application/x-ndjson")
//        )
      }

//      private def messageToJsonString(message: WriteMessage[T, C]): String =
//        message.operation match {
//          case Index =>
//            "\n" + writer.convert(message.source.get)
//          case Upsert =>
//            "\n" + JsObject(
//              "doc" -> writer.convert(message.source.get).parseJson,
//              "doc_as_upsert" -> JsTrue
//            ).toString
//          case Update =>
//            "\n" + JsObject(
//              "doc" -> writer.convert(message.source.get).parseJson
//            ).toString
//          case Delete =>
//            ""
//        }

      setHandlers(in, out, this)

      override def onPull(): Unit = tryPull()

      override def onPush(): Unit = {
        val message = grab(in)
        queue.enqueue(message)

        state match {
          case Idle => {
            state = Sending
            val messages = (1 to settings.bufferSize).flatMap { _ =>
              queue.dequeueFirst(_ => true)
            }
            sendBulkUpdateRequest(messages)
          }
          case _ => ()
        }

        tryPull()
      }

      override def onUpstreamFailure(exception: Throwable): Unit =
        failStage(exception)

      override def onUpstreamFinish(): Unit =
        state match {
          case Idle => handleSuccess()
          case Sending => state = Finished
          case Finished => ()
        }
    }
}

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] object ElasticsearchFlowStage {

  private object RetrySend

  private sealed trait State
  private case object Idle extends State
  private case object Sending extends State
  private case object Finished extends State

}
