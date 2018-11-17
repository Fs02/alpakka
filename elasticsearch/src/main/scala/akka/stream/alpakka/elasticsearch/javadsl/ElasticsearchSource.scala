/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.javadsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.javadsl.Source
import com.fasterxml.jackson.databind.ObjectMapper
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.search.builder.SearchSourceBuilder

/**
 * Java API to create Elasticsearch sources.
 */
object ElasticsearchSource {

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of [[java.util.Map]].
   * Using default objectMapper
   */
  def create(indexName: String,
             typeName: String,
             settings: ElasticsearchSourceSettings,
             client: RestHighLevelClient): Source[ReadResult[java.util.Map[String, Object]], NotUsed] =
    create(indexName, typeName, new SearchSourceBuilder(), settings, client, new ObjectMapper())

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of [[java.util.Map]].
   * Using custom objectMapper.
   *
   * Example of searchParams-usage:
   *
   * Map<String, String> searchParams = new HashMap<>();
   * searchParams.put("query", "{\"match_all\": {}}");
   * searchParams.put("_source", "[\"fieldToInclude\", \"anotherFieldToInclude\"]");
   */
  def create(indexName: String,
             typeName: String,
             searchSourceBuilder: SearchSourceBuilder,
             settings: ElasticsearchSourceSettings,
             client: RestHighLevelClient,
             objectMapper: ObjectMapper): Source[ReadResult[java.util.Map[String, Object]], NotUsed] =
    Source.fromGraph(
      new impl.ElasticsearchSourceStage(
        indexName,
        Option(typeName),
        searchSourceBuilder,
        client,
        settings,
        new JacksonReader[java.util.Map[String, Object]](objectMapper, classOf[java.util.Map[String, Object]])
      )
    )

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`.
   * Using default objectMapper
   */
  def typed[T](indexName: String,
               typeName: String,
               settings: ElasticsearchSourceSettings,
               client: RestHighLevelClient,
               clazz: Class[T]): Source[ReadResult[T], NotUsed] =
    typed[T](indexName, typeName, new SearchSourceBuilder(), settings, client, clazz, new ObjectMapper())

  /**
   * Creates a [[akka.stream.javadsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`.
   * Using custom objectMapper
   *
   * Example of searchParams-usage:
   *
   * Map<String, String> searchParams = new HashMap<>();
   * searchParams.put("query", "{\"match_all\": {}}");
   * searchParams.put("_source", "[\"fieldToInclude\", \"anotherFieldToInclude\"]");
   */
  def typed[T](indexName: String,
               typeName: String,
               searchSourceBuilder: SearchSourceBuilder,
               settings: ElasticsearchSourceSettings,
               client: RestHighLevelClient,
               clazz: Class[T],
               objectMapper: ObjectMapper): Source[ReadResult[T], NotUsed] =
    Source.fromGraph(
      new impl.ElasticsearchSourceStage(
        indexName,
        Option(typeName),
        searchSourceBuilder,
        client,
        settings,
        new JacksonReader[T](objectMapper, clazz)
      )
    )

  private final class JacksonReader[T](mapper: ObjectMapper, clazz: Class[T]) extends MessageReader[T] {

    override def convert(json: String): T = mapper.readValue(json, clazz)

  }

}
