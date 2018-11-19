/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.Flow
import org.elasticsearch.client.RestHighLevelClient

import scala.collection.JavaConverters._

/**
 * Java API to create Elasticsearch flows.
 */
object ElasticsearchFlow {

  /**
   * Creates a [[akka.stream.javadsl.Flow]] for type `T` from [[WriteMessage]] to lists of [[WriteResult]].
   */
  def create(
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      client: RestHighLevelClient
  ): akka.stream.javadsl.Flow[WriteMessage[String, NotUsed], java.util.List[WriteResult[String, NotUsed]], NotUsed] =
    create(indexName, typeName, settings, client, identity)

  /**
   * Creates a [[akka.stream.javadsl.Flow]] for type `T` from [[WriteMessage]] to lists of [[WriteResult]].
   */
  def create[T](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      client: RestHighLevelClient,
      writer: T => String
  ): akka.stream.javadsl.Flow[WriteMessage[T, NotUsed], java.util.List[WriteResult[T, NotUsed]], NotUsed] =
    Flow
      .fromGraph(
        new impl.ElasticsearchFlowStage[T, NotUsed](indexName, typeName, client, settings, writer)
      )
      .mapAsync(1)(identity)
      .map(x => x.asJava)
      .asJava

  /**
   * Creates a [[akka.stream.javadsl.Flow]] for type `T` from [[WriteMessage]] to lists of [[WriteResult]]
   * with `passThrough` of type `C`.
   */
  def createWithPassThrough[C](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      client: RestHighLevelClient
  ): akka.stream.javadsl.Flow[WriteMessage[String, C], java.util.List[WriteResult[String, C]], NotUsed] =
    createWithPassThrough(indexName, typeName, settings, client, identity)

  /**
   * Creates a [[akka.stream.javadsl.Flow]] for type `T` from [[WriteMessage]] to lists of [[WriteResult]]
   * with `passThrough` of type `C`.
   */
  def createWithPassThrough[T, C](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      client: RestHighLevelClient,
      writer: T => String
  ): akka.stream.javadsl.Flow[WriteMessage[T, C], java.util.List[WriteResult[T, C]], NotUsed] =
    Flow
      .fromGraph(
        new impl.ElasticsearchFlowStage[T, C](indexName, typeName, client, settings, writer)
      )
      .mapAsync(1)(identity)
      .map(x => x.asJava)
      .asJava

}
