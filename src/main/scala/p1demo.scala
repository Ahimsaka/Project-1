import org.apache.spark.sql.SparkSession

object p1demo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("P1 Demo")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

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


    // Problem Scenario 2.2
    // What is the least consumed beverage on Branch2
    println("Problem Scenario 2.2")
    println("What is the least consumed beverage on Branch2")
    spark.sql("SELECT c.product, SUM(c.consumers) total FROM branch b\nLEFT JOIN consumers c\nON b.product = c.product\nWHERE b.branch = \"Branch2\"\nGROUP BY c.product\nORDER BY total ASC\nLIMIT 1").show()


    // Problem Scenario 3.2
    // what are the comman beverages available in Branch4,Branch7?
    println("Problem Scenario 3.2")
    println("what are the common beverages available in Branch4,Branch7?")
    spark.sql("WITH b4 AS (SELECT product FROM branch WHERE branch = \"Branch4\")," +
    " b7 AS (SELECT product FROM branch WHERE branch = \"Branch7\")" +
    " SELECT DISTINCT(b4.product) product FROM b4" +
      " INNER JOIN b7" +
    " ON b4.product = b7.product").show()


    // OBSERVATION 1: Why Foreign Keys Are Essential
    println("OBSERVATION 1: Why Foreign Keys Are Essential")
    println("First, we will search for the sum total of SMALL_espresso sold on Branch1")
    val branch1 = spark.sql("SELECT b.branch, c.product, SUM(c.consumers) total FROM (SELECT DISTINCT(*) FROM branch) b JOIN consumers c ON b.product = c.product WHERE b.branch =  \"Branch1\" AND c.product =\"SMALL_Espresso\" GROUP BY b.branch, c.product")

    println("Next, let's search for the sum total of SMALL_espresso sold on Branch9 (the only other branch that carries it")
    val branch9 = spark.sql("SELECT b.branch, c.product, SUM(c.consumers) total FROM (SELECT DISTINCT(*) FROM branch) b JOIN consumers c ON b.product = c.product WHERE b.branch =  \"Branch9\" AND c.product =\"SMALL_Espresso\" GROUP BY b.branch, c.product")

    branch1.show
    branch9.show

    println("is the result set for branch 1 equal to the result set for branch9?")
    print("(branch1.collect()(0)(2) == branch9.collect()(0)(2)) evaluates to: ")
    println((branch1.collect()(0)(2) == branch9.collect()(0)(2)).toString)

    println("\nBecause the conscount tables do not have foreign keys relating to the branch table, there's no way to tell which conscount entries are from which branch! The branch data is functionally useless, because we can only find sales numbers for products overall. To normalize this data, we should have one table of branches, one table of products, and one table of sales with foreign keys in both ")

    // OBSERVATION 2: Hazards of Duplicate Data (Particularly Without Foreign Keys!)
    println("OBSERVATION 2: Hazards of Duplicate Data (Particularly Without Foreign Keys!)\n\n")
    println("The following entry appears in 2 of our input files, Bev_BranchC.txt AND Bev_BranchA.txt: \n \"Small_Espresso, Branch9\"")
    println("Because of this, if we join the tables WITHOUT selecting UNIQUE records first, we create duplicate data. Watch:")

    println("Query 1 - SELECTING DISTINCT \"SELECT b.branch, c.product, SUM(c.consumers) total FROM (SELECT DISTINCT(*) FROM branch) b JOIN consumers c ON b.product = c.product WHERE b.branch =  \"Branch9\" AND c.product =\"SMALL_Espresso\" GROUP BY b.branch, c.product\"")
    branch9.show

    println("Query 2 - WITHOUT SELECTING DISTINCT \"SELECT b.branch, c.product, SUM(c.consumers) total FROM branch b JOIN consumers c ON b.product = c.product WHERE b.branch =  \"Branch9\" AND c.product =\"SMALL_Espresso\" GROUP BY b.branch, c.product\"")
    spark.sql("SELECT b.branch, c.product, SUM(c.consumers) total FROM branch b JOIN consumers c ON b.product = c.product WHERE b.branch =  \"Branch9\" AND c.product =\"SMALL_Espresso\" GROUP BY b.branch, c.product").show

    println("Query 3 - Without Filtering Branch: \"SELECT product, SUM(consumers) FROM consumers WHERE product = \"SMALL_Espresso\" GROUP BY product\"")
    spark.sql("SELECT product, SUM(consumers) FROM consumers WHERE product = \"SMALL_Espresso\" GROUP BY product").show()

    println("\n\n As you can see, without selecting distinct, a query for SMALL_Espresso sales on Branch9 returns DOUBLE the total consumer count for SMALL_Espresso overall. If we aren't careful with data like this, we can accidentally multiply our output!")
  }
}
