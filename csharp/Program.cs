using System.Diagnostics;
using NetTopologySuite.Geometries;
using NetTopologySuite.Index.Strtree;
using NetTopologySuite.IO;
using Npgsql;

var env = File.ReadAllLines(".env")
    .Where(l => !string.IsNullOrWhiteSpace(l) && !l.StartsWith('#'))
    .Select(l => l.Split('=', 2))
    .ToDictionary(p => p[0].Trim(), p => p[1].Trim());

var user = env["DB_USER"];
var pass = env["DB_PASSWORD"];

var db = new DbManager(user, pass, "localhost", "gis");
var sql1 = """SELECT uprn::text id, ST_AsText(geom) geom FROM os.open_uprn_white_horse""";
var sql2 = """SELECT postcode id, ST_AsText(geom) geom FROM os.code_point_open_white_horse""";

var uprn = db.GetTable(sql1);
var codepoint = db.GetTable(sql2);

var sw = Stopwatch.StartNew();
var out1 = NearestNeighbour(uprn, codepoint); // ~22sec
sw.Stop();
SaveCsv(out1, "csharp_all_vs_all.csv");
Console.WriteLine(sw.Elapsed);

sw.Restart();
var out2 = NearestNeighbour2(uprn, codepoint); // ~3.6sec
sw.Stop();
SaveCsv(out2, "csharp_tree.csv");
Console.WriteLine(sw.Elapsed);

static Dictionary<string, (string, double)> NearestNeighbour(
    Dictionary<string, Geometry> geomA,
    Dictionary<string, Geometry> geomB)
{
    // for each geometry a get entry of b with the lowest distance, then compute dist to save map
    return geomA.ToDictionary(
        a => a.Key,
        a =>
        {
            var knn = geomB.MinBy(b => (a.Value.Distance(b.Value), b.Key))!;
            return (knn.Key, a.Value.Distance(knn.Value));
        });
}

static Dictionary<string, (string, double)> NearestNeighbour2(
    Dictionary<string, Geometry> geomA,
    Dictionary<string, Geometry> geomB)
{
    var tree = new STRtree<Geometry>();
    foreach (var (_, g) in geomB)
        tree.Insert(g.EnvelopeInternal, g);
    tree.Build();

    var geomBReverse = new Dictionary<Geometry, string>(ReferenceEqualityComparer.Instance);
    foreach (var (key, g) in geomB)
        geomBReverse[g] = key;

    return geomA.ToDictionary(
        a => a.Key,
        a =>
        {
            var knnGeom = tree.NearestNeighbour(a.Value.EnvelopeInternal, a.Value, new GeometryItemDistance(), 100);
            var knn = knnGeom
                .Select(g => (geomBReverse[g], a.Value.Distance(g)))
                .MinBy(x => (x.Item2, x.Item1))!;
            return knn;
        });
}

static void SaveCsv(Dictionary<string, (string, double)> table, string name)
{
    using var writer = new StreamWriter(name);
    writer.WriteLine("origin,destination,distance");
    foreach (var (key, (dest, dist)) in table)
        writer.WriteLine(FormattableString.Invariant($"{key},\"{dest}\",{dist}"));
}

class DbManager(string user, string pass, string host, string db)
{
    private readonly string _connectionString = $"Host={host};Database={db};Username={user};Password={pass}";

    public Dictionary<string, Geometry> GetTable(string sql)
    {
        var geom = new Dictionary<string, Geometry>();
        var reader = new WKTReader();

        try
        {
            using var conn = new NpgsqlConnection(_connectionString);
            conn.Open();
            using var cmd = new NpgsqlCommand(sql, conn);
            using var rs = cmd.ExecuteReader();
            while (rs.Read())
                geom[rs.GetString(0)] = reader.Read(rs.GetString(1));
        }
        catch (Exception e)
        {
            Console.Error.WriteLine(e);
        }

        return geom;
    }
}
