use std::env;
use std::path::Path;
use log::{info, warn};
use tokio::io::{Error, ErrorKind};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, Region};
use aws_sdk_s3::error::{UploadPartErrorKind, PutObjectErrorKind, GetObjectErrorKind, HeadObjectErrorKind};
use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::model::StorageClass;
use aws_sdk_s3::output::CreateMultipartUploadOutput;
use aws_smithy_http::byte_stream::{ByteStream, Length};
use crate::stats::TimeStats;

const S3_MIN_CHUNK_SIZE: u64 = 5242880;
const S3_MAX_CHUNK_SIZE: u64 = 5368709120;
const S3_MAX_CHUNKS: u64 = 10000;

fn match_storage_class(x: &str) -> StorageClass {
    match x {
        "STANDARD" => StorageClass::Standard,
        "INT" | "INTELLIGENT_TIERING" => StorageClass::IntelligentTiering,
        _ => {
            warn!("UNKOWN storage class {}, use standard as default", x);
            StorageClass::Standard
        },
    }
}

pub struct TransferManager {
    client: Client,
    storage_class: StorageClass,
    mpu_chunk_size: u64,
}

impl TransferManager {

    pub async fn new(region: &str) -> Self {

        let region_provider = RegionProviderChain::first_try(Region::new(region.to_owned()))
                                                    .or_default_provider()
                                                    .or_else(Region::new("us-west-2"));

        let shared_config = aws_config::from_env().region(region_provider).load().await;
        let client = Client::new(&shared_config);

        let storage_class = env::var("S3LOGS_TRANSFORM_STORAGE_CLASS")
                            .map(|x| match_storage_class(&x))
                            .unwrap_or(StorageClass::Standard);

        let mpu_chunk_size = env::var("S3LOGS_TRANSFORM_MPU_CHUNK_SIZE")
                            .map(|x| x.parse::<u64>().unwrap_or_default())
                            .unwrap_or_default();

        Self {
            client: client,
            storage_class: storage_class,
            mpu_chunk_size: mpu_chunk_size,
        }
    }

    pub async fn upload_object(&self, from: &str, bucket: &str, key: &str, mut chunksz: u64) -> Result<(), Error> {

        // for both chunk size set in code and env,
        // we choose bigger one
        if self.mpu_chunk_size > chunksz {
            chunksz = self.mpu_chunk_size;
        }

        // override default chunk size
        let mut chunk_size = S3_MIN_CHUNK_SIZE;
        if chunksz > S3_MIN_CHUNK_SIZE {
            chunk_size = chunksz;
        }

        // check file size
        let path = Path::new(from);
        let file_size = tokio::fs::metadata(path)
            .await
            .map(|x| x.len())?;

        if file_size == 0 {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid of file size"));
        }
        if file_size > S3_MAX_CHUNK_SIZE * S3_MAX_CHUNKS {
            return Err(Error::new(ErrorKind::InvalidInput, format!("file size {} exceed 5TiB, too big", file_size)));
        }

        if file_size > chunk_size {
            let chunk_count = (file_size / chunk_size) + 1;
            if chunk_count > S3_MAX_CHUNKS {
                return Err(Error::new(ErrorKind::InvalidInput, "exceed max chunk count, pls increase your chunk size"));
            }
            return self.multipart_upload_object(from, bucket, key, file_size, chunk_size).await;
        }

        if file_size > S3_MAX_CHUNK_SIZE {
            return Err(Error::new(ErrorKind::InvalidInput,
                format!("file size {} can not exceed 5GiB for single PUT Object, pls make your chunk size {} smaller",
                    file_size, chunk_size)
                ));
        }
        return self.put_object(from, bucket, key).await;
    }

    pub async fn multipart_upload_object(&self, from: &str, bucket: &str, key: &str,
                file_size: u64, chunk_size: u64) -> Result<(), Error> {

        let multipart_upload_res: CreateMultipartUploadOutput = self.client
                                                                .create_multipart_upload()
                                                                .bucket(bucket)
                                                                .key(key)
                                                                .storage_class(self.storage_class.clone())
                                                                .send()
                                                                .await
                                                                .unwrap();
        let upload_id = multipart_upload_res.upload_id().unwrap();

        let mut chunk_count = (file_size / chunk_size) + 1;
        let mut size_of_last_chunk = file_size % chunk_size;
        if size_of_last_chunk == 0 {
            size_of_last_chunk = chunk_size;
            chunk_count -= 1;
        }
        info!("initial multipart for file {} size {} split to chunk size {} chunk count {}",
                from, file_size, chunk_size, chunk_count);

        let path = Path::new(from);
        let mut upload_parts: Vec<CompletedPart> = Vec::new();

        let mut stat = TimeStats::new();
        for chunk_index in 0..chunk_count {
            let this_chunk = if chunk_count - 1 == chunk_index {
                size_of_last_chunk
            } else {
                chunk_size
            };
            let stream = ByteStream::read_from()
                .path(path)
                .offset(chunk_index * chunk_size)
                .length(Length::Exact(this_chunk))
                .build()
                .await
                .unwrap();
            //Chunk index needs to start at 0, but part numbers start at 1.
            let part_number = (chunk_index as i32) + 1;
            let upload_part_res = self.client
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(upload_id)
                .body(stream)
                .part_number(part_number)
                .send()
                .await;
            if upload_part_res.is_err() {
                match upload_part_res {
                    Err(aws_sdk_s3::types::SdkError::ServiceError { err, .. }) => match err.kind {
                        UploadPartErrorKind::Unhandled(_) => {}
                        _ => {}
                    },
                    Err(e) => Err(e).unwrap(),
                    _ => panic!(),
                }

                return Err(Error::new(ErrorKind::Other, "failed to upload part"));
            }
            upload_parts.push(
                CompletedPart::builder()
                    .e_tag(upload_part_res.ok().unwrap().e_tag.unwrap_or_default())
                    .part_number(part_number)
                    .build(),
            );
        }
        let completed_multipart_upload: CompletedMultipartUpload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();

        let _complete_multipart_upload_res = self.client
            .complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();

        info!("multipart upload object {} success, cost: {}", from, stat.elapsed());

        Ok(())
    }

    pub async fn put_object(&self, from: &str, bucket: &str, key: &str) -> Result<(), Error> {

        let body = ByteStream::from_path(Path::new(from)).await;
        let mut stat = TimeStats::new();
        info!("start to put object {} to s3://{}/{}", from, bucket, key);
        let res = self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .storage_class(self.storage_class.clone())
            .body(body.unwrap())
            .send()
            .await;
        if res.is_err() {
            match &res {
                Err(aws_sdk_s3::types::SdkError::ServiceError { err, .. }) => match err.kind {
                    PutObjectErrorKind::Unhandled(_) => {}
                    _ => {}
                },
                Err(e) => {
                    warn!("put object return error {}", e);
                },
                _ => panic!(),
            }
            warn!("failed to put object {}, reason: {:?}", from, res);
            return Err(Error::new(ErrorKind::Other, "failed to upload part"));
        }

        info!("put object {} success, cost: {}", from, stat.elapsed());
        Ok(())
    }

    pub async fn download_object(&self, bucket: &str, key: &str) -> Result<ByteStream, Error> {

        let res = self.client.get_object()
                        .bucket(bucket)
                        .key(key)
                        .send()
                        .await;
        if res.is_err() {
            match res {
                Err(aws_sdk_s3::types::SdkError::ServiceError { err, .. }) => match err.kind {
                    GetObjectErrorKind::InvalidObjectState(_) => {}
                    GetObjectErrorKind::NoSuchKey(_) => {}
                    GetObjectErrorKind::Unhandled(_) => {}
                    _ => {}
                },
                Err(e) => {
                    warn!("get object return error {}", e);
                },
                _ => panic!(),
            }

            return Err(Error::new(ErrorKind::Other, "failed to get object from S3"));
        }
        return Ok(res.ok().unwrap().body);
    }

    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<(), Error> {

        let res = self.client.head_object()
                        .bucket(bucket)
                        .key(key)
                        .send().await;
        if res.is_err() {
            match res {
                Err(aws_sdk_s3::types::SdkError::ServiceError { err, .. }) => match err.kind {
                    HeadObjectErrorKind::NotFound(_) => {
                        return Err(Error::new(ErrorKind::NotFound, "object not found"));
                    }
                    HeadObjectErrorKind::Unhandled(_) => {}
                    _ => {}
                },
                Err(e) => {
                    warn!("head object return error {}", e);
                },
                _ => panic!(),
            }

            return Err(Error::new(ErrorKind::Other, "unhandled error"));
        }
        Ok(())
    }
}
