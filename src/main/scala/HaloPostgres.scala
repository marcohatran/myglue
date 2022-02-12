import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job, JsonOptions}
import org.apache.spark.sql.SparkSession

object HaloPostgres {
  def main(args: Array[String]): Unit = {

    // to properly use this in aws glue, add a log4j.properties file to the s3 location referenced by
    // the --extra-files special parameter on the glue job
//    val logger = new GlueLogger

    // start simulation
    val options = GlueArgParser.getResolvedOptions(
      args,
      Array("show")
    )

    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("Java try")
      .master("local[2]")
      .getOrCreate()

    val glue = new GlueContext(spark.sparkContext)
//    glue
//      .getSource(
//        connectionType = "postgresql",
//        connectionOptions = JsonOptions(
//          Map(
//            "url"      -> "jdbc:postgresql://localhost:5432/postgres",
//            "dbtable"  -> "baz",
//            "user"     -> "postgres",
//            "password" -> "changeme",
//            "useSSL"   -> "false"
//          )
//        )
//      )
//      .getDynamicFrame()
//      .toDF()
//      .show()
    val datasource0 = glue.getSourceWithFormat(
      connectionType="postgresql",
      options =JsonOptions(s"""{
      "url":"jdbc:postgresql://52.197.242.109:5432/glue_test",
      "dbtable": "bar.baz",
      "redshiftTmpDir":"",
      "user":"hatt",
      "password":"123456a"
      }"""),
      transformationContext = "datasource0").getDynamicFrame().toDF().show()



    Job.commit()
  }

}
