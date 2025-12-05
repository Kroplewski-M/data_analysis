use chrono::NaiveDate;
use csv::{ReaderBuilder, WriterBuilder};
use regex::Regex;
use std::error::Error;

#[derive(Debug)]
struct Record {
    country: String,
    product: String,
    units_sold: i64,
    manufacturing_price: String,
    sale_price: f64,
    date: NaiveDate,
}
fn parse_money(s: &str) -> Option<f64> {
    // Remove $, £, commas
    let cleaned_string = s.replace(&['$', '£', ','][..], "");
    let clean = cleaned_string.trim(); // now safe, 'cleaned_string' lives long enough
    if clean.is_empty() || clean == "null" {
        None
    } else {
        clean.parse::<f64>().ok()
    }
}
fn clean_dashboard_csv() -> Result<(), Box<dyn Error>> {
    println!("Opening file");

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .trim(csv::Trim::All)
        .flexible(true)
        .from_path("Data/Part_B_Dashboard_file.csv")?;

    // Regex to clean $ and commas from numbers
    let re_dollar = Regex::new(r"[\$,]")?;

    let mut records = Vec::new();

    for result in rdr.records() {
        let record = result?;

        // Skip rows that are all nulls or empty
        if record
            .iter()
            .all(|s| s.trim().is_empty() || s.trim() == "null")
        {
            continue;
        }

        // Extract and clean columns
        let country = record.get(1).unwrap_or("").trim().to_string();
        let product = record.get(2).unwrap_or("").trim().to_string();

        // Skip row if Units Sold is empty or null
        let units_sold_str = record.get(4).unwrap_or("").trim();
        let units_sold = match parse_money(units_sold_str) {
            Some(val) => val.floor() as i64,
            None => continue,
        };

        // Skip row if Manufacturing Price is empty or null
        let manufacturing_price_raw = record.get(5).unwrap_or("").trim();
        if manufacturing_price_raw.is_empty() || manufacturing_price_raw == "null" {
            continue;
        }
        let manufacturing_price = re_dollar
            .replace_all(manufacturing_price_raw, "£")
            .to_string();

        // Skip row if Sale Price is empty or null
        let sale_price_str = record.get(6).unwrap_or("").trim();
        let sale_price = match parse_money(sale_price_str) {
            Some(val) => val,
            None => continue,
        };

        // Skip row if Date is empty or null
        let date_str = record.get(12).unwrap_or("").trim();
        if date_str.is_empty() || date_str == "null" {
            continue;
        }
        let date = NaiveDate::parse_from_str(date_str, "%d/%m/%Y")
            .or_else(|_| NaiveDate::parse_from_str(date_str, "%m/%d/%Y"))?;

        // Only push record if all fields are valid
        records.push(Record {
            country,
            product,
            units_sold,
            manufacturing_price,
            sale_price,
            date,
        });
    }

    println!("Calculating outlier bounds");

    let sale_prices: Vec<f64> = records.iter().map(|r| r.sale_price).collect();
    let mut sorted_prices = sale_prices.clone();
    sorted_prices.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let q1 = sorted_prices[(sorted_prices.len() as f64 * 0.25).floor() as usize];
    let q3 = sorted_prices[(sorted_prices.len() as f64 * 0.75).floor() as usize];
    let iqr = q3 - q1;
    let lower = q1 - 1.5 * iqr;
    let upper = q3 + 1.5 * iqr;

    println!("Filtering out outliers");

    let filtered: Vec<&Record> = records
        .iter()
        .filter(|r| r.sale_price >= lower && r.sale_price <= upper)
        .collect();

    println!("Saving cleaned CSV");

    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path("Data/Part_B_Dashboard_Cleaned.csv")?;

    // Write header
    wtr.write_record([
        "Country",
        "Product",
        "Units Sold",
        "Manufacturing Price Parsed",
        "Sale Price Parsed",
        "Date_ISO",
    ])?;

    for r in filtered {
        wtr.write_record([
            &r.country,
            &r.product,
            &r.units_sold.to_string(),
            &r.manufacturing_price,
            &format!("£{:.2}", r.sale_price),
            &r.date.format("%Y-%m-%d").to_string(),
        ])?;
    }

    wtr.flush()?;
    println!("Done!");

    Ok(())
}
fn main() -> Result<(), Box<dyn Error>> {
    clean_dashboard_csv()?;
    Ok(())
}
