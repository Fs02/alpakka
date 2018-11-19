/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import java.util.concurrent.CompletionStage

import akka.stream.alpakka.elasticsearch._
import akka.stream.javadsl._
import akka.{Done, NotUsed}
import org.elasticsearch.client.RestHighLevelClient

/**
 * Java API to create Elasticsearch sinks.
 */
object ElasticsearchSink {

  /**
   * Creates a [[akka.stream.javadsl.Sink]] to Elasticsearch for [[WriteMessage]] containing type `T`.
   */
  def create(
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      client: RestHighLevelClient
  ): akka.stream.javadsl.Sink[WriteMessage[String, NotUsed], CompletionStage[Done]] =
    create(indexName, typeName, settings, client, identity)

  /**
   * Creates a [[akka.stream.javadsl.Sink]] to Elasticsearch for [[WriteMessage]] containing type `T`.
   */
  def create[T](
      indexName: String,
      typeName: String,
      settings: ElasticsearchWriteSettings,
      client: RestHighLevelClient,
      writer: T => String
  ): akka.stream.javadsl.Sink[WriteMessage[T, NotUsed], CompletionStage[Done]] =
    ElasticsearchFlow
      .create(indexName, typeName, settings, client, writer)
      .toMat(Sink.ignore[java.util.List[WriteResult[T, NotUsed]]], Keep.right[NotUsed, CompletionStage[Done]])

}
