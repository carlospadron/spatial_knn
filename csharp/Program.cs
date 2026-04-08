using System.Diagnostics;
using NetTopologySuite.Geometries;
using NetTopologySuite.Index.Strtree;
using NetTopologySuite.IO;
using Npgsql;

const double MaxDistance = 5000.0;

var mode = args.Length > 0 ? args[0] : "both";

var user = Environment.GetEnvironmentVariable("DB_USER") ?? "postgres";
var pass = Environment.GetEnvironmentVariable("DB_PASSWORD") ?? "";
var host = Environment.GetEnvironmentVariable("DB_HOST") ?? "localhost";
var dbName = Environment.GetEnvironmentVariable("DB_NAME") ?? "gis";

var db = new DbManager(user, pass, host, dbName);
var uprnTable = Environment.GetEnvironmentVariable("UPRN_TABLE") ?? "os.open_uprn_white_horse";
var codepointTable = Environment.GetEnvironmentVariable("CODEPOINT_TABLE") ?? "os.code_point_open_white_horse";
var sql1 = $"SELECT uprn::text id, ST_AsText(geom) geom FROM {uprnTable}";
var sql2 = $"SELECT postcode id, ST_AsText(geom) geom FROM {codepointTable}";

var uprn = db.GetTable(sql1);
var codepoint = db.GetTable(sql2);

var timingsLines = new List<string> { "test,elapsed_s" };

if (mode is "brute" or "both")
{
    var sw = Stopwatch.StartNew();
    var out1 = NearestNeighbour(uprn, codepoint);
    sw.Stop();
    SaveCsv(out1, "csharp_all_vs_all.csv");
    timingsLines.Add($"C# all vs all,{sw.Elapsed.TotalSeconds}");
}

if (mode is "tree" or "both")
{
    var sw = Stopwatch.StartNew();
    var out2 = NearestNeighbour2(uprn, codepoint);
    sw.Stop();
    SaveCsv(out2, "csharp_tree.csv");
    timingsLines.Add($"C# strtree,{sw.Elapsed.TotalSeconds}");
}

File.WriteAllLines("csharp/timings.csv", timingsLines);

static Dictionary<string, (string, double)> NearestNeighbour(
    Dictionary<string, Geometry> geomA,
    Dictionary<string, Geometry> geomB)
{
    // for each geometry a get entry of b with the lowest distance, then compute dist to save map
    var result = new Dictionary<string, (string, double)>();
    foreach (var a in geomA)
    {
        var knn = geomB.MinBy(b => (a.Value.Distance(b.Value), b.Key))!;
        var dist = a.Value.Distance(knn.Value);
        if (dist <= MaxDistance)
            result[a.Key] = (knn.Key, dist);
    }
    return result;
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

    return geomA.Select(a =>
        {
            var knnGeom = tree.NearestNeighbour(a.Value.EnvelopeInternal, a.Value, new GeometryItemDistance(), 100);
            var knn = knnGeom
                .Select(g => (geomBReverse[g], a.Value.Distance(g)))
                .Aggregate((best, cur) =>
                    Math.Abs(cur.Item2 - best.Item2) < 1e-9
                        ? (string.Compare(cur.Item1, best.Item1, StringComparison.Ordinal) < 0 ? cur : best)
                        : (cur.Item2 < best.Item2 ? cur : best));
            return (a.Key, knn);
        })
        .Where(x => x.knn.Item2 <= MaxDistance)
        .ToDictionary(x => x.Key, x => x.knn);
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
