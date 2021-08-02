import org.apache.spark.sql.SparkSession

object p1demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("P1 Demo")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    println("Spark Session Engaged. You're the man now!")

    // create branch table
    spark.sql("CREATE TABLE IF NOT EXISTS branch (product STRING, branch STRING) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE branch")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE branch")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE branch")

    // create conscount table
    spark.sql("CREATE TABLE IF NOT EXISTS consumers (product STRING, consumers INT) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE consumers")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE consumers")
    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE consumers")

    // Problem Scenario 3.2
    // what are the comman beverages available in Branch4,Branch7?

    spark.sql("WITH b4 AS (SELECT product FROM branch WHERE branch = \"Branch4\")," +
    " b7 AS (SELECT product FROM branch WHERE branch = \"Branch7\")" +
    " SELECT DISTINCT(b4.product) product FROM b4" +
      " INNER JOIN b7" +
    " ON b4.product = b7.product").show()


    // Problem Scenario 2.2
    // What is the least consumed beverage on Branch2
    spark.sql("SELECT c.product, SUM(c.consumers) total FROM branch b\nLEFT JOIN consumers c\nON b.product = c.product\nWHERE b.branch = \"Branch2\"\nGROUP BY c.product\nORDER BY total ASC\nLIMIT 1").show()
  }
}
