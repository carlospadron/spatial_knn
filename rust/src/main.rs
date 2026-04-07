use postgres::{Client, NoTls};
use wkt::TryFromWkt;
use geo::Point;
use geo::EuclideanDistance;
use std::collections::HashMap;
use std::time::Instant;
use std::error::Error;
use csv::Writer;
use rstar::RTree;

fn db_manager(
    user: &String, 
    pass: &String,
    host: &str,
    db: &str,
    sql: &str
) -> HashMap<String, Point> {

    let url = format!("host={} user={} password={} dbname={}", host, user, pass, db);
    let mut client = Client::connect(
        &url,
        NoTls).unwrap();
    
    let mut geom:HashMap<String, Point> = HashMap::new();

    for row in client.query(sql, &[]).unwrap() {
        geom.insert(
            row.get(0),
            Point::try_from_wkt_str(row.get(1)).unwrap()
        );
        
    }

    geom
}

const MAX_DISTANCE: f64 = 5000.0;

fn nearest_neighbour(geoma: &HashMap<String, Point>, geomb: &HashMap<String, Point>) -> HashMap<String, (String, f64)>
{
    geoma.iter().filter_map(
        |(uprn, point1)| {
            let min = geomb.iter().min_by(
                |(postcode, point2), (postcode2, point22)| {
                    let dista = point1.euclidean_distance(*point2);
                    let distb = point1.euclidean_distance(*point22);
                    (dista, postcode).partial_cmp(&(distb, &postcode2)).unwrap()
                }).unwrap();
            let dist = point1.euclidean_distance(min.1);
            if dist > MAX_DISTANCE { return None; }
            Some((uprn.clone(), (min.0.clone(), dist)))
        }
    ).collect()
}

fn nearest_neighbour2(geoma: &HashMap<String, Point>, geomb: &HashMap<String, Point>) -> HashMap<String, (String, f64)>
{
    let mut reverse: HashMap<[u64; 2], Vec<&String>> = HashMap::new();
    for (name, pt) in geomb.iter() {
        reverse.entry([pt.x().to_bits(), pt.y().to_bits()])
            .or_default()
            .push(name);
    }
    let tree: RTree<Point<_>> = RTree::bulk_load(geomb.values().cloned().collect::<Vec<_>>());
    geoma.iter().filter_map(
        |(uprn, point)| {
            let mut nearest_iter = tree.nearest_neighbor_iter(point);
            let first = nearest_iter.next().unwrap();
            let min_dist = point.euclidean_distance(first);
            if min_dist > MAX_DISTANCE { return None; }
            let mut candidates: Vec<&String> = reverse[&[first.x().to_bits(), first.y().to_bits()]].clone();
            for pt in nearest_iter {
                if point.euclidean_distance(pt) > min_dist {
                    break;
                }
                candidates.extend(&reverse[&[pt.x().to_bits(), pt.y().to_bits()]]);
            }
            let postcode = candidates.iter().min().unwrap();
            Some((uprn.clone(), ((*postcode).clone(), min_dist)))
        }
    ).collect()
}

fn write_csv(output: HashMap<String, (String, f64)>, path: String) -> Result<(), Box<dyn Error>> {
    let mut wtr = Writer::from_path(path)?;

    wtr.write_record(&["origin", "destination", "distance"])?;
    output.iter().for_each(
        |(uprn, (postcode, distance))| {
            wtr.write_record(&[uprn.to_string(), postcode.to_string(), distance.to_string()]).unwrap();
        }
    );

    wtr.flush()?;
    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(|s| s.as_str()).unwrap_or("both");

    let user = std::env::var("DB_USER").unwrap_or_else(|_| "postgres".to_string());
    let password = std::env::var("DB_PASSWORD").unwrap_or_else(|_| "".to_string());
    let host = std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let db = std::env::var("DB_NAME").unwrap_or_else(|_| "gis".to_string());

    let uprn_table = std::env::var("UPRN_TABLE").unwrap_or_else(|_| "os.open_uprn_white_horse".to_string());
    let codepoint_table = std::env::var("CODEPOINT_TABLE").unwrap_or_else(|_| "os.code_point_open_white_horse".to_string());
    let sql = format!("SELECT uprn::text, ST_AsText(geom) FROM {}", uprn_table);
    let uprn = db_manager(&user, &password, &host, &db, &sql);
    let sql = format!("SELECT postcode, ST_AsText(geom) FROM {}", codepoint_table);
    let codepoint = db_manager(&user, &password, &host, &db, &sql);

    let mut wtr = Writer::from_path("timings.csv").unwrap();
    wtr.write_record(&["test", "elapsed_s"]).unwrap();

    if mode == "brute" || mode == "both" {
        let start = Instant::now();
        let output = nearest_neighbour(&uprn, &codepoint);
        let duration = start.elapsed();
        write_csv(output, "rust_all_vs_all.csv".to_owned()).unwrap();
        wtr.write_record(&["Rust all vs all", &duration.as_secs_f64().to_string()]).unwrap();
    }

    if mode == "tree" || mode == "both" {
        let start = Instant::now();
        let output = nearest_neighbour2(&uprn, &codepoint);
        let duration = start.elapsed();
        write_csv(output, "rust_tree.csv".to_owned()).unwrap();
        wtr.write_record(&["Rust strtree", &duration.as_secs_f64().to_string()]).unwrap();
    }

    wtr.flush().unwrap();
}
