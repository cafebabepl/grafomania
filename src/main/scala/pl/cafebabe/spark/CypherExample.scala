package pl.cafebabe.spark

import org.opencypher.morpheus.api.MorpheusSession
import org.opencypher.morpheus.api.io.{MorpheusNodeTable, MorpheusRelationshipTable}
import org.opencypher.okapi.api.graph.PropertyGraph

object CypherExample {

  def main(args: Array[String]): Unit = {
    implicit val morpheus: MorpheusSession = MorpheusSession.local()
    val spark = morpheus.sparkSession

    import spark.sqlContext.implicits._

    // dataframe, z których utworzymy wierzchołki i krawędzie
    // oczywiście w praktyce pobierzemy je np. z pliku (HDFS, S3), bazy danych (może relacyjnej) albo bezpośrednio z neo4j
    val employeesDF = spark.createDataset(Seq(
      (0L, "Marian", 32000),
      (1L, "Włodzimierz", 4000),
      (2L, "Piotr", 5099),
      (3L, "Witold", 21000),
      (4L, "Stanisław", 23000)
    )).toDF("id", "name", "salary")
    val likesDF = spark.createDataset(Seq(
      (0L, 1L, 0L, 5),
      (1L, 1L, 2L, 5),
      (2L, 1L, 3L, 5),
      (3L, 1L, 4L, 5),
      (4L, 2L, 3L, 4),
      (5L, 3L, 2L, 4),
      (6L, 3L, 0L, 5),
      (7L, 4L, 4L, 6)
    )).toDF("id", "source", "target", "rating")

    // wierzchołki typu (z etykietą) Employee
    val employees = MorpheusNodeTable(Set("Employee"), employeesDF)
    // krawędzie (relacje) typu :LIKES
    val likes = MorpheusRelationshipTable("LIKES", likesDF)

    // utworzenie grafu
    val graph: PropertyGraph = morpheus.readFrom(employees, likes)

    println("kto z kim się lubi?")
    val result1 = graph.cypher(
      """
        |MATCH (a:Employee)-[:LIKES]->(b:Employee)
        |RETURN a.name, collect(b.name)
        |""".stripMargin)
    result1.show

    println("pracownicy, którzy niewiele zarabiają ale bardzo lubią Witolda")
    graph.cypher(
      """
        |MATCH (a:Employee)-[r:LIKES]->(b:Employee)
        |WHERE a.salary < 5200 AND b.name = 'Witold' and r.rating >= 4
        |RETURN a.name, a.salary, r.rating
        |""".stripMargin)
      .show
  }

}
