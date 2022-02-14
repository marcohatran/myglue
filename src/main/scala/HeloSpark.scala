import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object HeloSpark {
  def main(args: Array[String]): Unit = {

    val logger = new GlueLogger

    // start simulation
    val options = GlueArgParser.getResolvedOptions(
      args,
      Array("show")
    )

    implicit val spark: SparkSession = SparkSession
      .builder()
      .getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    val glue = new GlueContext(spark.sparkContext)
    val gluedf: DataFrame = glue.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5430/my_haha")
      .option("user", "postgres")
      .option("password", "changeme")
      .option("dbtable", "myshce.alpha")
      .option("driver", "org.postgresql.Driver")
      .load()

    gluedf.show()
//    gluedf.write
//      .format("jdbc")
//      .option("url", "jdbc:postgresql://localhost:5430/my_haha")
//      .option("user", "postgres")
//      .option("password", "changeme")
//      .option("dbtable", "myshce.alpha_copy")
//      .option("driver", "org.postgresql.Driver")
//      .save()



//    val df: DataFrame = sqlContext.read
//      .format("jdbc")
//      .option("url", "jdbc:postgresql://localhost:5430/my_haha")
//      .option("user", "postgres")
//      .option("password", "changeme")
//      .option("dbtable", "myshce.alpha")
//      .option("driver", "org.postgresql.Driver")
//      .load()
//
//    df.show()

//    df.write
//      .format("jdbc")
//      .option("url", "jdbc:postgresql://localhost:5430/my_haha")
//      .option("user", "postgres")
//      .option("password", "changeme")
//      .option("dbtable", "myshce.alpha")
//      .option("driver", "org.postgresql.Driver")

//    val glue = new GlueContext(spark.sparkContext)
//
//    logger.info(s"System properties says, running ${System.getProperty("spark.app.name")} on ${System.getProperty("spark.master")}")
//    logger.info(s"Glue says, running ${glue.sparkContext.appName} on ${glue.sparkContext.master}")
//
//
//    val characters = glue.sparkContext.parallelize(Seq(
//      ("vince", "chase"),
//      ("john", "drama"),
//      ("turtle", null),
//      ("eric", "murphy"),
//      ("sloan", "mcquewick")
//    ))
//
//    val charactersDf = glue.createDataFrame(characters).toDF("firstName", "lastName")
//
//    charactersDf
//      .filter("lastName is not null")
//      .withColumnRenamed("firstName", "first name")
//      .withColumnRenamed("lastName", "last name")
//      .show()
    Job.commit()
  }

}
