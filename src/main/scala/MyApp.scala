import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import org.apache.spark.SparkContext

import java.sql.{Connection, DriverManager}
import scala.collection.JavaConverters._


object MyApp {
  def main(sysArgs: Array[String]) {

    // your normal glue etl scripts
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    //    ...


    //  postgresql connection sample
    var sql_connection: Connection = null
    Class.forName("org.postgresql.Driver")
    sql_connection = DriverManager.getConnection(
      "jdbc:postgresql://52.197.242.109:5432/glue_test", "hatt", "123456a")

    val prepare_statement = sql_connection.prepareStatement(s"select * from bar.baz")
    prepare_statement.executeUpdate()
    prepare_statement.close()


    Job.commit()
  }

}
