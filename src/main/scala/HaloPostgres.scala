import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, JsonOptions}
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
      .getOrCreate()

    val glue = new GlueContext(spark.sparkContext)
    glue
      .getSource(
        connectionType = "postgresql",
        connectionOptions = JsonOptions(
          Map(
            "url"      -> "jdbc:postgresql://localhost:5432/postgres",
            "dbtable"  -> "baz",
            "user"     -> "postgres",
            "password" -> "changeme",
            "useSSL"   -> "false"
          )
        )
      )
      .getDynamicFrame()
      .toDF()
      .show()
//    val datasource0 = glue.getSourceWithFormat(
//      connectionType="postgresql",
//      options =JsonOptions(s"""{
//      "url":"jdbc:postgresql://localhost:5432/postgres",
//      "dbtable": "public.company",
//      "user":"postgres",
//      "password":"changeme",
//      "useSSL": "false"
//      }"""),
//      transformationContext = "datasource0").getDynamicFrame().toDF().show()



//    Job.commit()
  }

}
