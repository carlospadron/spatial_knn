package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/tidwall/rtree"
)

type point struct{ x, y float64 }

type entry struct {
	id   string
	geom point
}

func dist(a, b point) float64 {
	dx, dy := a.x-b.x, a.y-b.y
	return math.Sqrt(dx*dx + dy*dy)
}

func distSq(a, b point) float64 {
	dx, dy := a.x-b.x, a.y-b.y
	return dx*dx + dy*dy
}

func parseWKT(wkt string) point {
	// "POINT(x y)"
	inner := strings.TrimSuffix(strings.TrimPrefix(wkt, "POINT("), ")")
	parts := strings.Fields(inner)
	x, _ := strconv.ParseFloat(parts[0], 64)
	y, _ := strconv.ParseFloat(parts[1], 64)
	return point{x, y}
}

func fetchData(connStr, sql string) []entry {
	conn, err := pgx.Connect(context.Background(), connStr)
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var entries []entry
	for rows.Next() {
		var id, wkt string
		if err := rows.Scan(&id, &wkt); err != nil {
			panic(err)
		}
		entries = append(entries, entry{id, parseWKT(wkt)})
	}
	return entries
}

type result struct {
	origin, destination string
	distance            float64
}

func allVsAll(uprn, codepoint []entry) []result {
	results := make([]result, len(uprn))
	for i, u := range uprn {
		bestDist := math.Inf(1)
		bestID := ""
		for _, c := range codepoint {
			d := dist(u.geom, c.geom)
			if d < bestDist || (d == bestDist && c.id < bestID) {
				bestDist = d
				bestID = c.id
			}
		}
		results[i] = result{u.id, bestID, bestDist}
	}
	return results
}

func strtreeKNN(uprn, codepoint []entry) []result {
	type item struct {
		id   string
		geom point
	}

	var tr rtree.RTreeG[item]
	for _, c := range codepoint {
		pt := [2]float64{c.geom.x, c.geom.y}
		tr.Insert(pt, pt, item{c.id, c.geom})
	}

	results := make([]result, len(uprn))
	for i, u := range uprn {
		pt := [2]float64{u.geom.x, u.geom.y}
		bestDist := math.Inf(1)
		bestDistSq := math.Inf(1) // BoxDist returns squared distance; keep separate for cutoff
		bestID := ""
		tr.Nearby(
			rtree.BoxDist[float64, item](pt, pt, nil),
			func(min, max [2]float64, c item, rtreeDist float64) bool {
				// rtreeDist is squared distance; stop when lower bound exceeds best squared dist.
				if rtreeDist > bestDistSq {
					return false
				}
				dSq := distSq(u.geom, c.geom)
				d := math.Sqrt(dSq)
				if d < bestDist || (d == bestDist && c.id < bestID) {
					bestDist = d
					bestDistSq = dSq // use raw sum-of-squares, not d*d, to avoid sqrt roundtrip error
					bestID = c.id
				}
				return true
			},
		)
		results[i] = result{u.id, bestID, bestDist}
	}
	return results
}

func saveCsv(results []result, path string) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fmt.Fprintln(f, "origin,destination,distance")
	for _, r := range results {
		fmt.Fprintf(f, "%s,\"%s\",%s\n", r.origin, r.destination,
			strconv.FormatFloat(r.distance, 'f', -1, 64))
	}
}

func main() {
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		dbName = "gis"
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, password, host, port, dbName)

	uprn := fetchData(connStr, "SELECT uprn::text, ST_AsText(geom) FROM os.open_uprn_white_horse")
	codepoint := fetchData(connStr, "SELECT postcode, ST_AsText(geom) FROM os.code_point_open_white_horse ORDER BY postcode")

	t1 := time.Now()
	out1 := allVsAll(uprn, codepoint)
	d1 := time.Since(t1)
	saveCsv(out1, "go_all_vs_all.csv")

	t2 := time.Now()
	out2 := strtreeKNN(uprn, codepoint)
	d2 := time.Since(t2)
	saveCsv(out2, "go_tree.csv")

	tf, err := os.Create("timings.csv")
	if err != nil {
		panic(err)
	}
	defer tf.Close()
	fmt.Fprintln(tf, "test,elapsed_s")
	fmt.Fprintf(tf, "Go all vs all,%f\n", d1.Seconds())
	fmt.Fprintf(tf, "Go strtree,%f\n", d2.Seconds())
}
