use chrono::NaiveDate;
use csv::{ReaderBuilder, WriterBuilder};
use std::error::Error;

#[derive(Debug)]
struct DashboardRow {
    country: String,
    product: String,
    units_sold: i64,
    manufacturing_price: f64,
    sale_price: f64,
    date: NaiveDate,
}
fn parse_money(s: &str) -> Option<f64> {
    // Remove $, £, commas
    let cleaned_string = s.replace(&['$', '£', ','][..], "");
    let clean = cleaned_string.trim();
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
        .from_path("Data/Part_B_Dashboard_file.csv")?;

    let mut records = Vec::new();

    for result in rdr.records() {
        let record = result?;

        if record
            .iter()
            .all(|s| s.trim().is_empty() || s.trim() == "null")
        {
            continue;
        }

        let country = record.get(1).unwrap_or("").trim().to_string();
        let product = record.get(2).unwrap_or("").trim().to_string();

        let units_sold_str = record.get(4).unwrap_or("").trim();
        let units_sold = match parse_money(units_sold_str) {
            Some(val) => val.floor() as i64,
            None => continue,
        };

        let manufacturing_price_raw = record.get(5).unwrap_or("").trim();
        if manufacturing_price_raw.is_empty() || manufacturing_price_raw == "null" {
            continue;
        }
        let manufacturing_price = match parse_money(manufacturing_price_raw) {
            Some(val) => val,
            None => continue,
        };

        let sale_price_str = record.get(6).unwrap_or("").trim();
        let sale_price = match parse_money(sale_price_str) {
            Some(val) => val,
            None => continue,
        };

        let date_str = record.get(12).unwrap_or("").trim();
        if date_str.is_empty() || date_str == "null" {
            continue;
        }
        let date = NaiveDate::parse_from_str(date_str, "%d/%m/%Y")?;

        records.push(DashboardRow {
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

    let filtered: Vec<&DashboardRow> = records
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
            &r.manufacturing_price.to_string(),
            &r.sale_price.to_string(),
            &r.date.format("%Y-%m-%d").to_string(),
        ])?;
    }

    wtr.flush()?;
    println!("Done!");

    Ok(())
}

fn clean_timeseries_csv() -> Result<(), Box<dyn Error>> {
    println!("Opening timeseries file");

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_path("Data/Part_C_Timeseries.csv")?;

    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path("Data/Part_C_Timeseries_Cleaned.csv")?;

    wtr.write_record([
        "Segment",
        "Country",
        "Product",
        "Discount Band",
        "Units Sold",
        "Manufacturing Price Parsed",
        "Sale Price Parsed",
        "Budget Parsed",
        "Discounts Parsed",
        "Sales Parsed",
        "COGS Parsed",
        "Profit Parsed",
        "Date_ISO",
    ])?;
    for result in rdr.records() {
        let record = result?;

        if record
            .iter()
            .all(|s| s.trim().is_empty() || s.trim() == "null")
        {
            continue;
        }

        let segment = record.get(0).unwrap_or("").trim().to_string();
        let country = record.get(1).unwrap_or("").trim().to_string();
        let product = record.get(2).unwrap_or("").trim().to_string();
        let discount_band = record.get(3).unwrap_or("").trim().to_string();

        let units_sold = parse_money(record.get(4).unwrap_or("").trim())
            .map(|v| v.floor() as i64)
            .unwrap_or(0);

        let manufacturing_price = parse_money(record.get(5).unwrap_or("").trim()).unwrap_or(0.0);
        let sale_price = parse_money(record.get(6).unwrap_or("").trim()).unwrap_or(0.0);
        let budget = parse_money(record.get(7).unwrap_or("").trim()).unwrap_or(0.0);
        let discounts = parse_money(record.get(8).unwrap_or("").trim()).unwrap_or(0.0);
        let sales = parse_money(record.get(9).unwrap_or("").trim()).unwrap_or(0.0);
        let cogs = parse_money(record.get(10).unwrap_or("").trim()).unwrap_or(0.0);
        let profit = parse_money(record.get(11).unwrap_or("").trim()).unwrap_or(0.0);

        let date_str = record.get(12).unwrap_or("").trim();
        if date_str.is_empty() || date_str == "null" {
            continue;
        }
        let date = NaiveDate::parse_from_str(date_str, "%d/%m/%Y")?;

        wtr.write_record([
            &segment,
            &country,
            &product,
            &discount_band,
            &units_sold.to_string(),
            &manufacturing_price.to_string(),
            &sale_price.to_string(),
            &budget.to_string(),
            &discounts.to_string(),
            &sales.to_string(),
            &cogs.to_string(),
            &profit.to_string(),
            &date.format("%Y-%m-%d").to_string(),
        ])?;
    }

    wtr.flush()?;
    println!("Timeseries CSV cleaned and saved!");

    Ok(())
}

fn clean_forcasting_csv() -> Result<(), Box<dyn Error>> {
    println!("Opening forcasting file");

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_path("Data/Part_D_Forcasting.csv")?;

    let mut wtr = WriterBuilder::new()
        .has_headers(true)
        .from_path("Data/Part_D_Forcasting_Cleaned.csv")?;

    wtr.write_record([
        "Segment",
        "Country",
        "Product",
        "Discount Band",
        "Units Sold",
        "Procurement",
        "Manufacturing Price Parsed",
        "Sale Price Parsed",
        "Budget Parsed",
        "Discounts Parsed",
        "Sales Parsed",
        "COGS Parsed",
        "Date_ISO",
    ])?;

    for result in rdr.records() {
        let record = result?;

        if record
            .iter()
            .all(|s| s.trim().is_empty() || s.trim() == "null")
        {
            continue;
        }
        let segment = record.get(0).unwrap_or("").trim().to_string();
        let country = record.get(1).unwrap_or("").trim().to_string();
        let product = record.get(2).unwrap_or("").trim().to_string();
        let discount_band = record.get(3).unwrap_or("").trim().to_string();

        let units_sold = parse_money(record.get(4).unwrap_or("").trim())
            .map(|v| v.floor() as i64)
            .unwrap_or(0);
        let procurment = parse_money(record.get(5).unwrap_or("").trim())
            .map(|v| v.floor() as i64)
            .unwrap_or(0);
        let manufactured_price = parse_money(record.get(6).unwrap_or("").trim())
            .map(|v| v.floor() as i64)
            .unwrap_or(0);
        let sale_price = parse_money(record.get(7).unwrap_or("").trim())
            .map(|v| v.floor() as i64)
            .unwrap_or(0);
        let budget = parse_money(record.get(8).unwrap_or("").trim())
            .map(|v| v.floor() as i64)
            .unwrap_or(0);
        let discounts = parse_money(record.get(9).unwrap_or("").trim())
            .map(|v| v.floor() as i64)
            .unwrap_or(0);
        let sales = parse_money(record.get(10).unwrap_or("").trim())
            .map(|v| v.floor() as i64)
            .unwrap_or(0);
        let cogs = parse_money(record.get(11).unwrap_or("").trim())
            .map(|v| v.floor() as i64)
            .unwrap_or(0);

        let date_str = record.get(12).unwrap_or("").trim();
        if date_str.is_empty() || date_str == "null" {
            continue;
        }
        let date = NaiveDate::parse_from_str(date_str, "%d/%m/%Y")?;

        wtr.write_record([
            &segment,
            &country,
            &product,
            &discount_band,
            &units_sold.to_string(),
            &procurment.to_string(),
            &manufactured_price.to_string(),
            &sale_price.to_string(),
            &budget.to_string(),
            &discounts.to_string(),
            &sales.to_string(),
            &cogs.to_string(),
            &date.format("%Y-%m-%d").to_string(),
        ])?;
    }
    println!("Forcasting CSV cleaned and saved!");
    Ok(())
}
fn main() -> Result<(), Box<dyn Error>> {
    //clean_dashboard_csv()?;
    //clean_timeseries_csv()?;
    clean_forcasting_csv()?;
    Ok(())
}
