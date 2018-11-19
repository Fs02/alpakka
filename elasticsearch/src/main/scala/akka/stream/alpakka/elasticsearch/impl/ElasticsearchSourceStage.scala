/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.elasticsearch.{ElasticsearchSourceSettings, ReadResult}
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler, StageLogging}
import akka.stream.{Attributes, Outlet, SourceShape}
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.search.{SearchRequest, SearchResponse, SearchScrollRequest}
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.search.builder.SearchSourceBuilder

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] case class ScrollResponse[T](error: Option[String], result: Option[ScrollResult[T]])

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] case class ScrollResult[T](scrollId: String, messages: Seq[ReadResult[T]])

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] final class ElasticsearchSourceStage[T](indexName: String,
                                                               typeName: Option[String],
                                                               searchSourceBuilder: SearchSourceBuilder,
                                                               client: RestHighLevelClient,
                                                               settings: ElasticsearchSourceSettings,
                                                               reader: String => T)
    extends GraphStage[SourceShape[ReadResult[T]]] {

  val out: Outlet[ReadResult[T]] = Outlet("ElasticsearchSource.out")
  override val shape: SourceShape[ReadResult[T]] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new ElasticsearchSourceLogic[T](indexName, typeName, searchSourceBuilder, client, settings, out, shape, reader)

}

/**
 * INTERNAL API
 */
@InternalApi
private[elasticsearch] final class ElasticsearchSourceLogic[T](indexName: String,
                                                               typeName: Option[String],
                                                               searchSourceBuilder: SearchSourceBuilder,
                                                               client: RestHighLevelClient,
                                                               settings: ElasticsearchSourceSettings,
                                                               out: Outlet[ReadResult[T]],
                                                               shape: SourceShape[ReadResult[T]],
                                                               reader: String => T)
    extends GraphStageLogic(shape)
    with ActionListener[SearchResponse]
    with OutHandler
    with StageLogging {

  private var scrollId: String = null
  private val responseHandler = getAsyncCallback[SearchResponse](handleResponse)
  private val failureHandler = getAsyncCallback[Throwable](handleFailure)

  private var waitingForElasticData = false
  private var pullIsWaitingForData = false
  private var dataReady: Option[ScrollResponse[T]] = None

  def sendScrollScanRequest(): Unit =
    try {
      waitingForElasticData = true

      if (scrollId == null) {
        log.debug("Doing initial search")

        if (searchSourceBuilder.size() < 0) {
          searchSourceBuilder.size(settings.bufferSize)
        }

        if (searchSourceBuilder.version() == null) {
          searchSourceBuilder.version(settings.includeDocumentVersion)
        }

        if (searchSourceBuilder.sorts() == null) {
          searchSourceBuilder.sort("_doc")
        }

        val searchRequest = new SearchRequest(indexName)
        searchRequest.source(searchSourceBuilder)
        searchRequest.scroll(TimeValue.timeValueMinutes(5))

        if (typeName.isDefined) {
          searchRequest.types(typeName.get)
        }

        client.searchAsync(searchRequest, this)
      } else {
        log.debug("Fetching next scroll")

        val scrollRequest = new SearchScrollRequest(scrollId)
        scrollRequest.scroll(TimeValue.timeValueMinutes(5))

        client.searchScrollAsync(scrollRequest, this)
      }
    } catch {
      case ex: Exception => handleFailure(ex)
    }

  override def onFailure(exception: Exception) = failureHandler.invoke(exception)
  override def onResponse(response: SearchResponse) = responseHandler.invoke(response)

  def handleFailure(ex: Throwable): Unit = {
    waitingForElasticData = false
    failStage(ex)
  }

  def handleResponse(res: SearchResponse): Unit = {
    waitingForElasticData = false

    val scrollResponse = if (res.getFailedShards > 0) {
      val error = res.getShardFailures.map(_.reason()).mkString(",")
      new ScrollResponse[T](Some(error), None)
    } else {
      val messages = res.getHits.getHits.map { hit =>
        val version = if (hit.getVersion < 0) None else Some(hit.getVersion)
        new ReadResult[T](hit.getId, reader(hit.getSourceAsString), version)
      }

      val scrollResult = ScrollResult[T](res.getScrollId, messages = messages)
      new ScrollResponse[T](None, Some(scrollResult))
    }

    if (pullIsWaitingForData) {
      log.debug("Received data from elastic. Downstream has already called pull and is waiting for data")
      pullIsWaitingForData = false
      if (handleScrollResponse(scrollResponse)) {
        // we should go and get more data
        sendScrollScanRequest()
      }
    } else {
      log.debug("Received data from elastic. Downstream have not yet asked for it")
      // This is a prefetch of data which we received before downstream has asked for it
      dataReady = Some(scrollResponse)
    }

  }

  // Returns true if we should continue to work
  def handleScrollResponse(scrollResponse: ScrollResponse[T]): Boolean =
    scrollResponse match {
      case ScrollResponse(Some(error), _) =>
        failStage(new IllegalStateException(error))
        false
      case ScrollResponse(None, Some(result)) if result.messages.isEmpty =>
        completeStage()
        false
      case ScrollResponse(_, Some(result)) =>
        scrollId = result.scrollId
        log.debug("Pushing data downstream")
        emitMultiple(out, result.messages.toIterator)
        true
    }

  setHandler(out, this)

  override def onPull(): Unit =
    dataReady match {
      case Some(data) =>
        // We already have data ready
        log.debug("Downstream is pulling data and we already have data ready")
        if (handleScrollResponse(data)) {
          // We should go and get more data

          dataReady = None

          if (!waitingForElasticData) {
            sendScrollScanRequest()
          }

        }
      case None =>
        if (pullIsWaitingForData) throw new Exception("This should not happen: Downstream is pulling more than once")
        pullIsWaitingForData = true

        if (!waitingForElasticData) {
          log.debug("Downstream is pulling data. We must go and get it")
          sendScrollScanRequest()
        } else {
          log.debug("Downstream is pulling data. Already waiting for data")
        }
    }

}
