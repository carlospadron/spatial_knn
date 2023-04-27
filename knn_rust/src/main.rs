use std::io;
use postgres::{Client, NoTls};
use wkt::TryFromWkt;
use geo::{Point, LineString};
use geo::EuclideanDistance;
use std::collections::HashMap;
use std::time::{Duration, Instant};
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

fn nearest_neighbour(geoma: &HashMap<String, Point>, geomb: &HashMap<String, Point>) -> HashMap<String, (String, f64)>
{
    geoma.iter().map(
        |(uprn, point1)| {
            let min = geomb.iter().min_by(
                |(postcode, point2), (postcode2, point22)| {
                    let dista = point1.euclidean_distance(point2.clone());
                    let distb = point1.euclidean_distance(point22.clone());
                    (dista, postcode).partial_cmp(&(distb, &postcode2)).unwrap()
                }).unwrap();
            (uprn.clone(), (min.0.clone(), point1.euclidean_distance(min.1)))
        }
    ).collect()
}
fn nearest_neighbour2(geoma: &HashMap<String, Point>, geomb: &HashMap<String, Point>) -> HashMap<String, (String, f64)>
{
    let geomb2 = geomb.clone();
    let tree_a: RTree<Point<_>> = RTree::bulk_load(geomb2.into_values().collect::<Vec<_>>());
    geoma.iter().map(
        |(uprn, point)| {
            let nearest = tree_a.nearest_neighbors(&point);
            let postcodes: Vec<(&String)> = nearest.iter().map(
                |point2|
                    geomb
                        .iter()
                        .find(|(s, p)| p==point2)
                        .unwrap()
                        .0
            ).collect();

            let postcode = postcodes.iter().min().unwrap().clone();
            (uprn.clone(), (postcode.clone(), point.euclidean_distance(geomb.get(postcode).unwrap())))
        }
    ).collect()
}

fn writeCsv(output: HashMap<String, (String, f64)>, path: String) -> Result<(), Box<dyn Error>> {
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
    let mut user = String::new();
    let mut password = String::new();

    println!("user");
    io::stdin().read_line(&mut user).unwrap();
    println!("password");
    io::stdin().read_line(&mut password).unwrap();

    let host = "localhost";
    let db = "gis";
    let sql = "SELECT \"UPRN\"::text, ST_AsText(geom) FROM os.open_uprn_white_horse";
    let uprn = db_manager(&user, &password, &host, &db, &sql);
    let sql = "SELECT \"postcode\", ST_AsText(geom) FROM os.code_point_open_white_horse ";
    let codepoint = db_manager(&user, &password, &host, &db, &sql);    

    let start = Instant::now();
    let output = nearest_neighbour(&uprn, &codepoint);
    let duration = start.elapsed();
    writeCsv(output, "rust_all_vs_all.csv".to_owned());
    println!("Time elapsed is: {:?}", duration);

    let start = Instant::now();
    let output = nearest_neighbour2(&uprn, &codepoint);
    let duration = start.elapsed();
    writeCsv(output, "rust_tree.csv".to_owned());
    println!("Time elapsed is: {:?}", duration);
}
