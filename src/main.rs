use polars::prelude::*;
use std::error::Error;
use std::fs::File;

fn read_json_to_dataframe(json_file: &str) -> Result<DataFrame, Box<dyn Error>> {
    // Open the JSON file and create a DataFrame using Polars' JsonReader
    let mut file = File::open(json_file)?;
    let df = JsonReader::new(&mut file).finish()?;
    Ok(df)
}

fn write_parquet(df: &mut DataFrame, parquet_file: &str) -> Result<(), Box<dyn Error>> {
    // Open the file for writing and create a ParquetWriter
    let mut file = File::create(parquet_file)?;
    ParquetWriter::new(&mut file).finish(df)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // Read JSON data into a DataFrame
    let mut df = read_json_to_dataframe("data.json")?;

    // Convert and write to Parquet
    write_parquet(&mut df, "output.parquet")?;

    println!("Successfully wrote Parquet file to: output.parquet");
    Ok(())
}
