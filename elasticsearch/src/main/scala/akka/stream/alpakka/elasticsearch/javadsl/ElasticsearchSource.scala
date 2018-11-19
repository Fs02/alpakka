/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.javadsl.Source
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.search.builder.SearchSourceBuilder

/**
 * Java API to create Elasticsearch sources.
 */
object ElasticsearchSource {

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`.
   * Using default objectMapper
   */
  def create(indexName: String,
             typeName: String,
             settings: ElasticsearchSourceSettings,
             client: RestHighLevelClient): Source[ReadResult[String], NotUsed] =
    typed(indexName, typeName, new SearchSourceBuilder(), settings, client, identity)

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`.
   * Using custom objectMapper
   */
  def create(indexName: String,
             typeName: String,
             searchSourceBuilder: SearchSourceBuilder,
             settings: ElasticsearchSourceSettings,
             client: RestHighLevelClient): Source[ReadResult[String], NotUsed] =
    typed(indexName, typeName, searchSourceBuilder, settings, client, identity)

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`.
   * Using default objectMapper
   */
  def typed[T](indexName: String,
               typeName: String,
               settings: ElasticsearchSourceSettings,
               client: RestHighLevelClient,
               reader: String => T): Source[ReadResult[T], NotUsed] =
    typed(indexName, typeName, new SearchSourceBuilder(), settings, client, reader)

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`.
   * Using custom objectMapper
   */
  def typed[T](indexName: String,
               typeName: String,
               searchSourceBuilder: SearchSourceBuilder,
               settings: ElasticsearchSourceSettings,
               client: RestHighLevelClient,
               reader: String => T): Source[ReadResult[T], NotUsed] =
    Source.fromGraph(
      new impl.ElasticsearchSourceStage(indexName, Option(typeName), searchSourceBuilder, client, settings, reader)
    )
}
