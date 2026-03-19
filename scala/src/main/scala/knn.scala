import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.strtree.{GeometryItemDistance, STRtree}
import org.locationtech.jts.io.WKTReader

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.{DriverManager, SQLException}
import scala.collection.immutable.HashMap
import scala.io.StdIn.readLine
class DbManager(
                 user: String,
                 pass: String,
                 host: String,
                 db: String
               ) :
  val connectionUrl = s"jdbc:postgresql://$host/$db?user=$user&password=$pass"

  def getTable(sql: String) =
    val geom = scala.collection.mutable.HashMap.empty[String, Geometry]
    val reader = WKTReader()

    try
      val connection = DriverManager.getConnection(connectionUrl)
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(sql)

      while resultSet.next do
              geom += resultSet.getString("id") -> reader.read(resultSet.getString("geom"))
      connection.close()
    catch
        case e: SQLException => e.printStackTrace

    geom.toMap

def nearestNeighbour(geoma: Map[String, Geometry], geomb: Map[String, Geometry]) =
  //for each geometry a get entry of b with the lowest distance, then compute dist to save map
  geoma
    .map(
      x =>
        val knn = geomb.minBy(b => (x._2.distance(b._2), b._1))
        (x._1 , knn._1, x._2.distance(knn._2))
    )
    .toList

def nearestNeighbour2(geoma: Map[String, Geometry], geomb: Map[String, Geometry]) =
  val t = STRtree()
  geomb.foreach( x =>
    t.insert( x._2.getEnvelopeInternal, x._2)
  )
  t.build()
  val geomb2 = geomb.map(_.swap)

  geoma.map (
    x =>
      val knnGeom = t.nearestNeighbour(x._2.getEnvelopeInternal, x._2, GeometryItemDistance(), 100).toList.asInstanceOf[List[Geometry]]
      val knn = knnGeom.map(y => (geomb2(y), x._2.distance(y))).minBy((x,y) => (y, x))

      (x._1, knn._1, knn._2)
    )
    .toList

def saveCsv(table: List[(String, String, Double)], name: String) =
  val file = File(s"$name")
  val writer = BufferedWriter(FileWriter(file))
  writer.write("""origin,destination,distance""")
  writer.newLine()
  table.foreach(
    x =>
      writer.write(s"${x(0)}, \"${x(1)}\", ${x(2)}")
      writer.newLine()
  )
  writer.flush()

@main def main() =
  println("db user")
  val user = readLine()
  println("bd password")
  val pass = readLine()

  val db = DbManager(user, pass, "localhost", "gis")
  val sql1 = """SELECT "UPRN" id, ST_AsText(geom) geom FROM os.open_uprn_white_horse"""
  val sql2 = """SELECT "postcode" id, ST_AsText(geom) geom FROM os.code_point_open_white_horse"""

  val uprn = db.getTable(sql1)
  val codepoint = db.getTable(sql2)

  val startTime = System.currentTimeMillis()
  val out1 = nearestNeighbour(uprn, codepoint) //22sec
  val endTime = System.currentTimeMillis()
  saveCsv(out1, "scala_all_vs_all.csv")

  println(startTime - endTime)
  val startTime2 = System.currentTimeMillis()
  val out2 = nearestNeighbour2(uprn, codepoint) //3.6sec
  val endTime2 = System.currentTimeMillis()
  saveCsv(out2, "scala_tree.csv")

  println(startTime2 - endTime2)