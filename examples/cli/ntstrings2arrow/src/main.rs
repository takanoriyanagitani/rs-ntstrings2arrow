use std::io;
use std::process::ExitCode;

use rs_ntstrings2arrow::arrow;

use arrow::array::DictionaryArray;
use arrow::datatypes::UInt32Type;

use rs_ntstrings2arrow::path2dict_arr_async_variable;

fn env2ntsname() -> Result<String, io::Error> {
    std::env::var("NTS_NAME").map_err(|e| io::Error::new(io::ErrorKind::NotFound, e))
}

async fn path2arr(p: &str, chunk_size: usize) -> Result<DictionaryArray<UInt32Type>, io::Error> {
    path2dict_arr_async_variable(p, None, chunk_size).await
}

fn env2chunk_size() -> Result<usize, io::Error> {
    std::env::var("CHUNK_SIZE")
        .map_err(io::Error::other)
        .and_then(|s| str::parse(&s).map_err(io::Error::other))
}

async fn sub() -> Result<(), io::Error> {
    let null_terminated_strings_filename: String = env2ntsname()?;
    let chunk_size: usize = env2chunk_size()?;

    let da: DictionaryArray<_> = path2arr(&null_terminated_strings_filename, chunk_size).await?;
    rs_ntstrings2arrow::print_da_info(&da)?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
