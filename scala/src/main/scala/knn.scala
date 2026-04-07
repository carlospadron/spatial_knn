import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.strtree.{GeometryItemDistance, STRtree}
import org.locationtech.jts.io.WKTReader

import java.io.{BufferedWriter, File, FileWriter}
import java.sql.{DriverManager, SQLException}
import java.util.Locale
import scala.collection.immutable.HashMap
class DbManager(
                 user: String,
                 pass: String,
                 host: String,
                 port: String,
                 db: String
               ) :
  val connectionUrl = s"jdbc:postgresql://$host:$port/$db?user=$user&password=$pass"

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
      val knn = knnGeom.map(y => (geomb2(y), x._2.distance(y))).reduceLeft { (a, b) =>
        if math.abs(a._2 - b._2) < 1e-9 then
          if a._1 < b._1 then a else b
        else if a._2 < b._2 then a else b
      }

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
      writer.write(s"${x(0)},\"${x(1)}\",${String.format(Locale.US, "%f", x(2))}")
      writer.newLine()
  )
  writer.flush()

@main def main(args: String*) =
  val mode = args.headOption.getOrElse("both")

  val user   = Option(System.getenv("DB_USER")).getOrElse("postgres")
  val pass   = Option(System.getenv("DB_PASSWORD")).getOrElse("")
  val host   = Option(System.getenv("DB_HOST")).getOrElse("localhost")
  val port   = Option(System.getenv("DB_PORT")).getOrElse("5432")
  val dbName = Option(System.getenv("DB_NAME")).getOrElse("gis")

  val db   = DbManager(user, pass, host, port, dbName)
  val uprnTable      = Option(System.getenv("UPRN_TABLE")).getOrElse("os.open_uprn_white_horse")
  val codepointTable = Option(System.getenv("CODEPOINT_TABLE")).getOrElse("os.code_point_open_white_horse")
  val sql1 = s"SELECT uprn::text id, ST_AsText(geom) geom FROM $uprnTable"
  val sql2 = s"SELECT postcode id, ST_AsText(geom) geom FROM $codepointTable"

  val uprn      = db.getTable(sql1)
  val codepoint = db.getTable(sql2)

  val timingsWriter = BufferedWriter(FileWriter(File("timings.csv")))
  timingsWriter.write("test,elapsed_s")
  timingsWriter.newLine()

  if mode == "brute" || mode == "both" then
    val startTime = System.currentTimeMillis()
    val out1 = nearestNeighbour(uprn, codepoint)
    val endTime = System.currentTimeMillis()
    saveCsv(out1, "scala_all_vs_all.csv")
    timingsWriter.write(s"Scala all vs all,${(endTime - startTime) / 1000.0}")
    timingsWriter.newLine()

  if mode == "tree" || mode == "both" then
    val startTime = System.currentTimeMillis()
    val out2 = nearestNeighbour2(uprn, codepoint)
    val endTime = System.currentTimeMillis()
    saveCsv(out2, "scala_tree.csv")
    timingsWriter.write(s"Scala strtree,${(endTime - startTime) / 1000.0}")
    timingsWriter.newLine()

  timingsWriter.flush()