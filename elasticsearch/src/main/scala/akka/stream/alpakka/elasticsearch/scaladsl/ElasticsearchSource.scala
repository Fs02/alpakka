/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.scaladsl

import akka.NotUsed
import akka.stream.alpakka.elasticsearch._
import akka.stream.scaladsl.Source
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.search.builder.SearchSourceBuilder
import spray.json._

/**
 * Scala API to create Elasticsearch sources.
 */
object ElasticsearchSource {

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s
   * of Spray's [[spray.json.JsObject]].
   * Alias of [[create]].
   */
  def apply(indexName: String,
            typeName: String,
            searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder(),
            settings: ElasticsearchSourceSettings = ElasticsearchSourceSettings.Default)(
      implicit client: RestHighLevelClient
  ): Source[ReadResult[JsObject], NotUsed] = create(indexName, typeName, searchSourceBuilder, settings)

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s
   * of Spray's [[spray.json.JsObject]].
   * Alias of [[create]].
   */
  def apply(indexName: String,
            typeName: Option[String],
            searchSourceBuilder: SearchSourceBuilder,
            settings: ElasticsearchSourceSettings)(
      implicit client: RestHighLevelClient
  ): Source[ReadResult[JsObject], NotUsed] = create(indexName, typeName, searchSourceBuilder, settings)

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s
   * of Spray's [[spray.json.JsObject]].
   */
  def create(indexName: String,
             typeName: String,
             searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder(),
             settings: ElasticsearchSourceSettings = ElasticsearchSourceSettings.Default)(
      implicit client: RestHighLevelClient
  ): Source[ReadResult[JsObject], NotUsed] =
    create(indexName, Option(typeName), searchSourceBuilder, settings)

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s
   * of Spray's [[spray.json.JsObject]].
   */
  def create(indexName: String,
             typeName: Option[String],
             searchSourceBuilder: SearchSourceBuilder,
             settings: ElasticsearchSourceSettings)(
      implicit client: RestHighLevelClient
  ): Source[ReadResult[JsObject], NotUsed] =
    Source.fromGraph(
      new impl.ElasticsearchSourceStage(
        indexName,
        typeName,
        searchSourceBuilder,
        client,
        settings,
        new SprayJsonReader[JsObject]()(DefaultJsonProtocol.RootJsObjectFormat)
      )
    )

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`
   * converted by Spray's [[spray.json.JsonReader]]
   */
  def typed[T](indexName: String,
               typeName: String,
               searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder(),
               settings: ElasticsearchSourceSettings = ElasticsearchSourceSettings.Default)(
      implicit client: RestHighLevelClient,
      reader: JsonReader[T]
  ): Source[ReadResult[T], NotUsed] =
    typed(indexName, Option(typeName), searchSourceBuilder, settings)

  /**
   * Creates a [[akka.stream.scaladsl.Source]] from Elasticsearch that streams [[ReadResult]]s of type `T`
   * converted by Spray's [[spray.json.JsonReader]]
   */
  def typed[T](indexName: String,
               typeName: Option[String],
               searchSourceBuilder: SearchSourceBuilder,
               settings: ElasticsearchSourceSettings)(
      implicit client: RestHighLevelClient,
      reader: JsonReader[T]
  ): Source[ReadResult[T], NotUsed] =
    Source.fromGraph(
      new impl.ElasticsearchSourceStage(indexName,
                                        typeName,
                                        searchSourceBuilder,
                                        client,
                                        settings,
                                        new SprayJsonReader[T]()(reader))
    )

  private final class SprayJsonReader[T](implicit reader: JsonReader[T]) extends MessageReader[T] {

    override def convert(json: String): T = json.parseJson.convertTo[T]

  }

}
