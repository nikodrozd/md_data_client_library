import akka.actor.CoordinatedShutdown.UnknownReason
import akka.actor.{ActorSystem, CoordinatedShutdown}
import com.here.hrn.HRN
import com.here.platform.data.client.scaladsl.DataClient

import scala.util.{Failure, Success}

object OverviewScalaMain {

  def main(args: Array[String]): Unit = {
    implicit val actorSystem = ActorSystem()
    implicit val dispatcher = actorSystem.dispatcher

    val dataClient = DataClient()
    val adminApi = dataClient.adminApi()

    // Now you can start using the API
    adminApi
//      .getConfiguration(HRN("hrn:here:data::olp-here:mddclctlg"))
      .listCatalogs()
      .andThen {
        case Success(catalogHRNs) => catalogHRNs.foreach(println)
        case Failure(e) => e.printStackTrace()
      }
      .andThen {
        case _ => CoordinatedShutdown(actorSystem).run(UnknownReason)
      }
  }
}