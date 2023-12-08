use log::warn;
use config::Config;
use parquet::basic::{Compression, GzipLevel, ZstdLevel, BrotliLevel};
use parquet::file::properties::WriterVersion;
use parquet::file::properties::WriterProperties;

const PARQUET_WRITER_DEFAULT_MAX_ROW_GROUP_SIZE: usize = 100000000;

pub struct ParquetWriterConfigReader;

fn match_compression(x: &str) -> Compression {
    match x {
        "UNCOMPRESSED" => Compression::UNCOMPRESSED,
        "SNAPPY" => Compression::SNAPPY,
        "GZIP" => Compression::GZIP(GzipLevel::default()),
        "LZO" => Compression::LZO,
        "BROTLI" => Compression::BROTLI(BrotliLevel::default()),
        "LZ4" => Compression::LZ4,
        "ZSTD" => Compression::ZSTD(ZstdLevel::default()),
        "LZ4_RAW" => Compression::LZ4_RAW,
        _ => {
            warn!("UNKOWN compression method {}, use default", x);
            Compression::SNAPPY
        },
    }
}

fn match_writer_version(x: &str) -> WriterVersion {
    match x {
        "1.0" => WriterVersion::PARQUET_1_0,
        "2.0" => WriterVersion::PARQUET_2_0,
        _ => {
            warn!("UNKOWN writer version {}, use default", x);
            WriterVersion::PARQUET_1_0
        },
    }
}

impl ParquetWriterConfigReader {

    pub fn default() -> WriterProperties {
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_created_by("aws s3logs writer".to_string())
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_max_row_group_size(PARQUET_WRITER_DEFAULT_MAX_ROW_GROUP_SIZE)
            .build()
    }

    pub fn new(fullpath: &str) -> WriterProperties {

        let res = Config::builder()
                        .add_source(config::File::with_name(fullpath))
                        .build();
        if res.is_err() {
            warn!("unable to read parquet writer config at {}, use default writer props", fullpath);
            return Self::default();
        }

        let res = res.unwrap().get_table("DEFAULT");
        if res.is_err() {
            warn!("unable to get DEFAULT section, use default writer props");
            return Self::default();
        }

        let config = res.unwrap();

        let compression = config.get("compression")
                                    .map(|x|
                                        x.to_owned().into_string()
                                            .map(|y| match_compression(&y))
                                            .unwrap()
                                    )
                                    .unwrap_or(Compression::SNAPPY);

        let created_by = config.get("created_by")
                                    .map(|x|
                                        x.to_owned().into_string()
                                        .unwrap()
                                    )
                                    .unwrap_or("aws s3logs writer".to_string());

        let writer_version = config.get("writer_version")
                                    .map(|x| 
                                        x.to_owned().into_string()
                                            .map(|y| match_writer_version(&y))
                                            .unwrap()
                                    )
                                    .unwrap_or(WriterVersion::PARQUET_1_0);

        let max_row_group_size = config.get("max_row_group_size")
                                    .map(|x|
                                        x.to_owned().into_uint()
                                            .map(|y| y as usize)
                                            .unwrap()
                                    )
                                    .unwrap_or(PARQUET_WRITER_DEFAULT_MAX_ROW_GROUP_SIZE as usize);

        WriterProperties::builder()
            .set_compression(compression)
            .set_created_by(created_by)
            .set_writer_version(writer_version)
            .set_max_row_group_size(max_row_group_size)
            .build()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn init() {
        let _ = env_logger::builder()
                        .filter(Some("s3logs"), log::LevelFilter::Debug)
                        .is_test(true)
                        .try_init();
    }

    #[test]
    fn test_load_config() {

        init();

        ParquetWriterConfigReader::new("/mnt/s3logs/config/parquet_writer_properties.ini");
    }
}
