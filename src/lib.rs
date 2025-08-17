pub use arrow;

use std::io;

use std::path::Path;

use std::borrow::Cow;
use std::ffi::CStr;

use futures::Stream;
use futures::TryStreamExt;

use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::BufReader;

use async_stream::try_stream;

use arrow::array::DictionaryArray;
use arrow::array::StringDictionaryBuilder;

use arrow::datatypes::ArrowDictionaryKeyType;

pub fn strings2dict_array_nullable<I, K>(
    strings: I,
    size_hint: Option<(usize, usize, usize)>,
) -> DictionaryArray<K>
where
    I: Iterator<Item = Option<String>>,
    K: ArrowDictionaryKeyType,
{
    let mut b = match size_hint {
        None => StringDictionaryBuilder::new(),
        Some(kvd) => StringDictionaryBuilder::with_capacity(kvd.0, kvd.1, kvd.2),
    };

    b.extend(strings);

    b.finish()
}

pub fn strings2dict_array<I, K>(
    strings: I,
    size_hint: Option<(usize, usize, usize)>,
) -> Result<DictionaryArray<K>, io::Error>
where
    I: Iterator<Item = Result<String, io::Error>>,
    K: ArrowDictionaryKeyType,
{
    let mut b = match size_hint {
        None => StringDictionaryBuilder::new(),
        Some(kvd) => StringDictionaryBuilder::with_capacity(kvd.0, kvd.1, kvd.2),
    };

    for rstr in strings {
        let s: String = rstr?;
        b.append(s).map_err(io::Error::other)?;
    }

    Ok(b.finish())
}

pub async fn nts2strings_lossy<R, const N: usize>(
    null_terminated_strings: R,
) -> impl Stream<Item = Result<String, io::Error>>
where
    R: AsyncRead + Unpin,
{
    let mut buf = [0u8; N];
    let mut br = BufReader::new(null_terminated_strings);

    try_stream! {
        loop {
            let rslt: Result<(), _> = br.read_exact(&mut buf).await.map(|_| ());
            let reof: Result<bool, _> = match rslt {
                Ok(_) => Ok(false),
                Err(e) => match e.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(true),
                    _ => Err(e),
                },
            };

            let eof: bool = reof?;
            if eof {
                return
            }

            let csr: &CStr = CStr::from_bytes_until_nul(&buf)
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "null‑terminated string missing NUL byte",
                    )
                })?;

            let cos: Cow<str> = csr.to_string_lossy();

            yield cos.into()
        }
    }
}

pub async fn nts2strings_lossy_variable<R>(
    null_terminated_strings: R,
    chunk_size: usize,
) -> impl Stream<Item = Result<String, io::Error>>
where
    R: AsyncRead + Unpin,
{
    let mut buf: Vec<u8> = vec![0u8; chunk_size];
    let mut br = BufReader::new(null_terminated_strings);

    try_stream! {
        loop {
            let b: &mut [u8] = &mut buf;
            let rslt: Result<(), _> = br.read_exact(b).await.map(|_| ());
            let reof: Result<bool, _> = match rslt {
                Ok(_) => Ok(false),
                Err(e) => match e.kind() {
                    io::ErrorKind::UnexpectedEof => Ok(true),
                    _ => Err(e),
                },
            };

            let eof: bool = reof?;
            if eof {
                return
            }

            let csr: &CStr = CStr::from_bytes_until_nul(&buf)
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "null‑terminated string missing NUL byte",
                    )
                })?;

            let cos: Cow<str> = csr.to_string_lossy();

            yield cos.into()
        }
    }
}

pub async fn strings2dict_array_async<S, K>(
    strings: S,
    size_hint: Option<(usize, usize, usize)>,
) -> Result<DictionaryArray<K>, io::Error>
where
    S: Stream<Item = Result<String, io::Error>>,
    K: ArrowDictionaryKeyType,
{
    let da = match size_hint {
        None => StringDictionaryBuilder::new(),
        Some(kvd) => StringDictionaryBuilder::with_capacity(kvd.0, kvd.1, kvd.2),
    };

    Ok(strings
        .try_fold(da, |mut state, next| async move {
            let s: String = next;
            state.append(s).map_err(io::Error::other)?;
            Ok(state)
        })
        .await?
        .finish())
}

pub async fn nts2dict_arr_async<R, K, const N: usize>(
    null_terminated_strings: R,
    size_hint: Option<(usize, usize, usize)>,
) -> Result<DictionaryArray<K>, io::Error>
where
    R: AsyncRead + Unpin,
    K: ArrowDictionaryKeyType,
{
    let string_stream = nts2strings_lossy::<_, N>(null_terminated_strings).await;
    strings2dict_array_async(string_stream, size_hint).await
}

pub async fn file2dict_arr_async<K, const N: usize>(
    null_terminated_strings: tokio::fs::File,
    size_hint: Option<(usize, usize, usize)>,
) -> Result<DictionaryArray<K>, io::Error>
where
    K: ArrowDictionaryKeyType,
{
    nts2dict_arr_async::<_, _, N>(null_terminated_strings, size_hint).await
}

pub async fn path2dict_arr_async<P, K, const N: usize>(
    null_terminated_strings: P,
    size_hint: Option<(usize, usize, usize)>,
) -> Result<DictionaryArray<K>, io::Error>
where
    P: AsRef<Path>,
    K: ArrowDictionaryKeyType,
{
    let f: tokio::fs::File = tokio::fs::File::open(null_terminated_strings).await?;
    file2dict_arr_async::<_, N>(f, size_hint).await
}

pub async fn nts2dict_arr_async_variable<R, K>(
    null_terminated_strings: R,
    chunk_size: usize,
    size_hint: Option<(usize, usize, usize)>,
) -> Result<DictionaryArray<K>, io::Error>
where
    R: AsyncRead + Unpin,
    K: ArrowDictionaryKeyType,
{
    let string_stream = nts2strings_lossy_variable::<_>(null_terminated_strings, chunk_size).await;
    strings2dict_array_async(string_stream, size_hint).await
}

pub async fn file2dict_arr_async_variable<R, K>(
    null_terminated_strings: R,
    chunk_size: usize,
    size_hint: Option<(usize, usize, usize)>,
) -> Result<DictionaryArray<K>, io::Error>
where
    R: AsyncRead + Unpin,
    K: ArrowDictionaryKeyType,
{
    nts2dict_arr_async_variable(null_terminated_strings, chunk_size, size_hint).await
}

pub async fn path2dict_arr_async_variable<P, K>(
    null_terminated_strings: P,
    size_hint: Option<(usize, usize, usize)>,
    chunk_size: usize,
) -> Result<DictionaryArray<K>, io::Error>
where
    P: AsRef<Path>,
    K: ArrowDictionaryKeyType,
{
    let f: tokio::fs::File = tokio::fs::File::open(null_terminated_strings).await?;
    file2dict_arr_async_variable::<_, K>(f, chunk_size, size_hint).await
}

pub fn print_da_info<K>(da: &DictionaryArray<K>) -> Result<(), io::Error>
where
    K: ArrowDictionaryKeyType,
{
    let keylen: usize = da.len();
    let uniqlen: usize = da.values().len();
    println!("[DictionaryArray] Number of keys: {keylen}");
    println!("[DictionaryArray] Number of unique values: {uniqlen}");
    Ok(())
}
