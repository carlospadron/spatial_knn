import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.strtree.GeometryItemDistance
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io.WKTReader
import java.io.File
import java.sql.DriverManager
import java.sql.SQLException


class DbManager(
    user: String,
    pass: String,
    host: String,
    db: String
) {
    private val connectionUrl = "jdbc:postgresql://$host/$db?user=$user&password=$pass"

    fun getTable(sql: String): Map<String, Geometry> {
        val geom = mutableMapOf<String, Geometry>()
        val reader = WKTReader()

        try {
            DriverManager.getConnection(this.connectionUrl).use {
                connection -> connection.createStatement().use {
                    statement -> statement.executeQuery(sql).use { resultSet ->
                        while (resultSet.next()) {
                            geom[resultSet.getString("id")] = reader.read(resultSet.getString("geom"))
                        }
                    }
                }
            }
        } catch (e: SQLException) {
            e.printStackTrace()
        }
        return geom.toMap()
    }
}
fun nearestNeighbour(geoma: Map<String, Geometry>, geomb: Map<String, Geometry>): Map<String, Pair<String?, Double>> {
    //for each geometry a get entry of b with the lowest distance, then compute dist to save map
    val dist: Map<String, Pair<String?, Double>> = geoma.map {
            a ->
                val knn = geomb.minWithOrNull(compareBy({ b -> a.value.distance(b.value)},{ b -> b.key}))
                a.key to (knn?.key to a.value.distance(knn?.value))
    }.toMap()
    return dist
}

fun nearestNeighbour2(geoma: Map<String, Geometry>, geomb: Map<String, Geometry>): Map<String, Pair<String?, Double>> {
    val t = STRtree()
    geomb.forEach {
        t.insert(it.value.envelopeInternal, it.value)
    }
    t.build()
    val geomb2 = geomb.map{ x -> x.value to x.key}.toMap()

    val dist = geoma.map {
            a ->
                val knnGeom = t.nearestNeighbour(a.value.envelopeInternal, a.value, GeometryItemDistance(), 100).toList() as List<Geometry>
                val knn = knnGeom.associate { y -> geomb2[y] to a.value.distance(y) }
                    .minWithOrNull(compareBy({ b -> b.value },{ b -> b.key}))
                a.key to (knn?.key to knn?.value)
    }.toMap() as Map<String, Pair<String?, Double>>

    return dist
}

fun saveCsv(table: Map<String, Pair<String?, Double>>, name: String) {
    val writer = File("$name").bufferedWriter()
    writer.write("""origin,destination,distance""")
    writer.newLine()
    table.forEach {
        writer.write("""${it.key}, "${it.value.first}", ${it.value.second}""")
        writer.newLine()
    }
    writer.flush()
}

fun main() {
    println("db user")
    val user = readln()
    println("bd password")
    val pass = readln()

    val db = DbManager(user, pass, "localhost", "gis")
    val sql1 = """SELECT "UPRN" id, ST_AsText(geom) geom FROM os.open_uprn_white_horse"""
    val sql2 = """SELECT "postcode" id, ST_AsText(geom) geom FROM os.code_point_open_white_horse"""

    val uprn = db.getTable(sql1)
    val codepoint = db.getTable(sql2)

    val startTime = System.currentTimeMillis()
    val out1 = nearestNeighbour(uprn, codepoint) //22sec
    val endTime = System.currentTimeMillis()
    saveCsv(out1, "kotlin_all_vs_all.csv")

    println(startTime - endTime)
    val startTime2 = System.currentTimeMillis()
    val out2 = nearestNeighbour2(uprn, codepoint) //3.6sec
    val endTime2 = System.currentTimeMillis()
    saveCsv(out2, "kotlin_tree.csv")

    println(startTime2 - endTime2)
}