import akka.actor.CoordinatedShutdown.UnknownReason
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.http.scaladsl.model.ContentTypes
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import akka.{Done, NotUsed}
import com.here.hrn.HRN
import com.here.platform.data.client.engine.model.StableBlobIdGenerator
import com.here.platform.data.client.engine.scaladsl.DataEngine
import com.here.platform.data.client.model.DeleteIndexesStatusResponse.{Failed, Succeeded}
import com.here.platform.data.client.model._
import com.here.platform.data.client.scaladsl._
import com.typesafe.config.ConfigFactory
import java.io.File
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.language.postfixOps


object CatalogActions extends App {
  implicit val actorSystem: ActorSystem = ActorSystem()
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher
  val catalogId = ConfigFactory.load().getString("here.md.data.catalog-id")
  val versionedLayerId = ConfigFactory.load().getString("here.md.data.versioned-layer-id")
  val volatileLayerId = ConfigFactory.load().getString("here.md.data.volatile-layer-id")
  val indexLayerId = ConfigFactory.load().getString("here.md.data.index-layer-id")
  val objectStoreLayerId = ConfigFactory.load().getString("here.md.data.object-store-layer-id")
  val streamLayerId = ConfigFactory.load().getString("here.md.data.stream-layer-id")
  val partitionId = ConfigFactory.load().getString("here.md.data.partition.id")
  val partitionFile = ConfigFactory.load().getString("here.md.data.partition.data-file-path")
  val queryString = ConfigFactory.load().getString("here.md.data.index-query-string")
  val catalogHrn = HRN(s"hrn:here:data::olp-here:$catalogId")

  for {
    hrn <- createCatalogWithLayers(catalogId, versionedLayerId, volatileLayerId, indexLayerId, objectStoreLayerId, streamLayerId)
    _ <- createVersionedLayerPartition(hrn, versionedLayerId, partitionId, partitionFile)
    _ <- createVolatileLayerPartition(hrn, volatileLayerId, partitionId, partitionFile)
    _ <- createIndexLayerPartition(hrn, indexLayerId, partitionId, partitionFile, Map("someTimeWindowKey" -> TimeWindowIndexValue(600000L)))
    _ <- createObjectInObjectStoreLayer(hrn, objectStoreLayerId, partitionId, partitionFile)
    versionedData <- getDataFromLayerByLayerId(hrn, versionedLayerId, VersionedLayerType)
    volatileData <- getDataFromLayerByLayerId(hrn, volatileLayerId, VolatileLayerType())
    //    indexData <- getDataFromIndexLayerByQuery(hrn, indexLayerId, queryString)
    objectStoreData <- getDataFromObjectStoreLayerByKey(hrn, objectStoreLayerId, partitionId)
  } yield {
    versionedData.runWith {
      Sink.foreach(println)
    }
    volatileData.runWith {
      Sink.foreach(println)
    }
    //    indexData.runWith {
    //      Sink.foreach(println)
    //    }
    objectStoreData.runForeach {
      (byteData: ByteString) => println(byteData.utf8String)
    }
  }
    .andThen {
      case _ =>
        for {
          indexDeleteResult <- deleteDataFromIndexLayerByQuery(catalogHrn, indexLayerId, queryString)
          osDeleteResult <- deleteDataFromObjectStoreLayerByKey(catalogHrn, objectStoreLayerId, partitionId)
        } yield {
          println(indexDeleteResult)
          println(osDeleteResult)
        }
    }
    .andThen {
      case _ => deleteCatalog(catalogHrn)
    }
    .andThen {
      case _ => CoordinatedShutdown(actorSystem).run(UnknownReason)
    }

  def createCatalogWithLayers(catalogId: String, versionedLayerId: String, volatileLayerId: String, indexLayerId: String,
                              objectStoreLayerId: String, streamLayerId: String): Future[HRN] = {
    val catalogConfig =
      WritableCatalogConfiguration(
        id = catalogId,
        name = catalogId,
        summary = "This is a catalog summary.",
        description = "This is what the catalog is for.",
        automaticVersionDeletion =
          Some(AutomaticVersionDeletion.builder.withNumberOfVersionsToKeep(10L).build),
        layers = Seq(
          WritableLayer(
            id = versionedLayerId,
            name = versionedLayerId,
            summary = "This is a layer summary.",
            description = "This is a layer description.",
            layerType = VersionedLayerType,
            partitioning = GenericPartitioning,
            volume = Volumes.Durable,
            contentType = "application/x-protobuf",
            digest = Some(DigestAlgorithm.MD5),
            crc = Some(CrcAlgorithm.CRC32C)
          ),
          WritableLayer(
            id = volatileLayerId,
            name = volatileLayerId,
            summary = "This is a layer summary.",
            description = "This is a layer description.",
            layerType = VolatileLayerType(),
            partitioning = HereTilePartitioning(tileLevels = 1 :: Nil),
            volume = Volumes.Durable,
            contentType = "application/x-protobuf",
            contentEncoding = Some(ContentEncoding.gzip),
            coverage = Some(Coverage(Set[String]("DE", "CN", "US")))
          ),
          WritableLayer(
            id = indexLayerId,
            name = indexLayerId,
            summary = "This is a layer summary.",
            description = "This is a layer description.",
            layerType = IndexLayerType(
              indexDefinitions = Seq(
                IndexDefinition("someTimeWindowKey",
                  IndexType.TimeWindow,
                  duration = Some(600000))
              ),
              ttl = Ttl.OneMonth
            ),
            partitioning = NoPartitioning,
            volume = Volumes.Durable,
            contentType = "application/x-protobuf",
            digest = Some(DigestAlgorithm.SHA256),
            crc = Some(CrcAlgorithm.CRC32C)
          ),
          WritableLayer(
            id = objectStoreLayerId,
            name = objectStoreLayerId,
            summary = "This is layer summary",
            description = "This is layer description",
            layerType = ObjectStoreLayerType,
            partitioning = NoPartitioning,
            volume = Volumes.Durable
          ),
          WritableLayer(
            id = streamLayerId,
            name = streamLayerId,
            summary = "This is layer summary",
            description = "This is layer description",
            layerType = StreamLayerType(streamProperties = Some(StreamProperties(
              dataInThroughputKbps = 1000,
              dataOutThroughputKbps = 4000,
              parallelization = Some(4)
            ))),
            contentType = "application/x-protobuf",
            partitioning = HereTilePartitioning(tileLevels = 1 :: Nil),
            volume = Volumes.Durable
          )
        )
      )
    val adminApi = DataClient().adminApi()
    adminApi.createCatalog(catalogConfig)
  }

  def createObjectInObjectStoreLayer(catalogHrn: HRN, objectStoreLayerId: String, key: String, dataFile: String): Future[String] = {
    val writeEngine = DataEngine().writeEngine(catalogHrn)

    writeEngine.uploadObject(objectStoreLayerId,
      key,
      NewPartition.FileBlob(new File(dataFile)),
      Some(ContentTypes.`application/json`)).flatMap {
      _ => Future.successful(key)
    }
  }

  def createIndexLayerPartition(catalogHrn: HRN, indexLayerId: String, partitionId: String, dataFile: String, attributes: Map[String, IndexValue]): Future[String] = {
    val writeEngine = DataEngine().writeEngine(catalogHrn)
    val newPartition = NewPartition(
      partition = partitionId,
      layer = indexLayerId,
      data = NewPartition.FileBlob(new File(dataFile)),
      fields = Some(attributes),
      metadata = None,
      checksum = None,
      crc = None,
      dataSize = None
    )

    writeEngine.uploadAndIndex(Iterator(newPartition)).flatMap {
      _ => Future.successful(partitionId)
    }
  }

  def createVolatileLayerPartition(catalogHrn: HRN, volatileLayerId: String, partitionId: String, dataFile: String): Future[String] = {
    val publishApi = DataClient().publishApi(catalogHrn)
    publishApi.getBaseVersion().flatMap { baseVersion =>
      val nextVersion =
        baseVersion
          .map(_ + 1L)
          .getOrElse(0L)

      val writeEngine =
        DataEngine().writeEngine(catalogHrn, new StableBlobIdGenerator(nextVersion))

      val dependencies = Seq.empty[VersionDependency]

      val partitions: Source[PendingPartition, NotUsed] =
        Source(
          List(
            NewPartition(
              partition = partitionId,
              layer = volatileLayerId,
              data = NewPartition.FileBlob(new File(dataFile))
            )
          )
        )

      val commitPartitions: Source[CommitPartition, NotUsed] =
        partitions.mapAsync(parallelism = 10) { pendingPartition =>
          writeEngine.put(pendingPartition)
        }

      publishApi
        .publishBatch2(baseVersion, Some(Seq(volatileLayerId)), dependencies, commitPartitions).flatMap {
        _ => Future.successful(partitionId)
      }
    }
  }

  def createVersionedLayerPartition(catalogHrn: HRN, versionLayerId: String, partitionId: String, dataFile: String): Future[String] = {
    val writeEngine = DataEngine().writeEngine(catalogHrn)
    val dependencies = Seq.empty[VersionDependency]
    val partitions: Source[PendingPartition, NotUsed] =
      Source(
        List(
          NewPartition(
            partition = partitionId,
            layer = versionLayerId,
            data = NewPartition.FileBlob(new File(dataFile))
          )
        )
      )

    writeEngine.publishBatch2(parallelism = 10,
      layerIds = Some(Seq(versionLayerId)),
      dependencies = dependencies,
      partitions = partitions).flatMap {
      _ => Future.successful(partitionId)
    }
  }

  def getDataFromObjectStoreLayerByKey(catalogHrn: HRN, objectStoreLayerId: String, key: String): Future[Source[ByteString, NotUsed]] = {
    val readEngine = DataEngine().readEngine(catalogHrn)
    readEngine.getObjectDataAsSource(objectStoreLayerId, key)
  }

  def getDataFromIndexLayerByQuery(catalogHrn: HRN, indexLayerId: String, queryString: String): Future[Source[Option[String], NotUsed]] = {
    import scala.concurrent.Await
    import scala.concurrent.duration._
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    val queryApi = DataClient().queryApi(catalogHrn)
    val readEngine = DataEngine().readEngine(catalogHrn)
    val numberOfParts = 50
    val parallelism = 4
    val indexParts = Await.result(queryApi.queryIndexParts(indexLayerId, numberOfParts), 1.seconds)

    val foundIndexPartitionsAsSource: Source[IndexPartition, NotUsed] = {
      Await.result(
        Future(Source(indexParts.parts)
          .mapAsync(parallelism) { part =>
            queryApi
              .queryIndex(indexLayerId, Some(queryString), Some(part))
          }
          .flatMapConcat(identity)), Duration.Inf
      )
    }

    Future.successful {
      foundIndexPartitionsAsSource
        .mapAsync(parallelism)(partition => readEngine
          .getDataAsBytes(partition).map(bytes => Some(new String(bytes)))
          .recover { case _ => None })
    }
  }

  def getDataFromLayerByLayerId(catalogHrn: HRN, layerId: String, layerType: LayerType): Future[Source[Option[String], NotUsed]] = {
    val queryApi = DataClient().queryApi(catalogHrn)
    val readEngine = DataEngine().readEngine(catalogHrn)

    def downloadData(partition: Partition): Future[Option[String]] =
      readEngine
        .getDataAsBytes(partition)
        .map(bytes => Some(new String(bytes)))
        .recover { case _ => None }

    val result: Future[Source[Partition, NotUsed]] = layerType match {
      case VersionedLayerType => queryApi.getPartitions(0L, layerId)
      case VolatileLayerType(_, _) => queryApi.getVolatilePartitions(volatileLayerId)
    }

    result.map { ps: Source[Partition, NotUsed] =>
      ps.mapAsync(parallelism = 10) { partition: Partition =>
        downloadData(partition)
      }
    }
  }

  def deleteDataFromObjectStoreLayerByKey(catalogHrn: HRN, objectStoreLayerId: String, key: String): Future[Done] = {
    val writeEngine = DataEngine().writeEngine(catalogHrn)
    writeEngine.deleteObject(objectStoreLayerId, key)
  }

  def deleteDataFromIndexLayerByQuery(catalogHrn: HRN, indexLayerId: String, queryString: String): Future[String] = {
    import scala.concurrent.Await
    import scala.concurrent.duration._

    val publishApi = DataClient().publishApi(catalogHrn)
    val queryApi = DataClient().queryApi(catalogHrn)
    val deleteId =
      Await.result(publishApi.deleteIndex(indexLayerId, queryString), 45.seconds)

    @tailrec
    def checkResult(response: DeleteIndexesStatusResponse): String = {
      response.currentState match {
        case Succeeded => s"Partition with id = ${response.deleteId} has been deleted successfully"
        case Failed => response.message
        case _ => checkResult(Await.result(queryApi.queryIndexDeleteStatus(indexLayerId, deleteId), 5.seconds))

      }
    }

    val deleteStatusResponse: DeleteIndexesStatusResponse =
      Await.result(queryApi.queryIndexDeleteStatus(indexLayerId, deleteId), 5.seconds)
    Future(checkResult(deleteStatusResponse))
  }

  def deleteCatalog(catalogHrn: HRN): Future[HRN] = {
    val adminApi = DataClient().adminApi()
    adminApi.deleteCatalog(catalogHrn).flatMap { _ =>
      Future.successful(catalogHrn)
    }
  }
}
