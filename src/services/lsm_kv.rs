use lsm_kv::common::profier::WriteProfiler;
use lsm_kv::compact::CompactionOptions;
use lsm_kv::storage::lsm_storage::WriteBatchRecord;
use lsm_kv::storage::LsmKV;
use volo_gen::plumedb::{FillReq, FillResp, GetReq, GetResp, KeyValePair, ScanReq, ScanResp};
use volo_grpc::Status;

pub struct LsmKVServiceImpl<T: CompactionOptions>(LsmKV<T>);

impl<T: CompactionOptions> volo_gen::plumedb::LsmKvService for LsmKVServiceImpl<T> {
    async fn fill(
        &self,
        req: ::volo_grpc::Request<FillReq>,
    ) -> ::std::result::Result<::volo_grpc::Response<FillResp>, Status> {
        let FillReq { pairs } = req.into_inner();
        let write_batch = pairs
            .into_iter()
            .map(|pair| {
                let KeyValePair { key, value } = pair;
                WriteBatchRecord::Put(key, value)
            })
            .collect::<Vec<_>>();
        let mut profiler = WriteProfiler::default();
        self.0
            .write_bytes_batch_with_profier(&mut profiler, &write_batch)
            .map_err(|e| Status::internal(format!("{e}")))?;
        todo!("response grpc message")
    }

    async fn get(
        &self,
        req: ::volo_grpc::Request<GetReq>,
    ) -> std::result::Result<::volo_grpc::Response<GetResp>, ::volo_grpc::Status> {
        todo!()
    }

    async fn scan(
        &self,
        req: ::volo_grpc::Request<ScanReq>,
    ) -> std::result::Result<::volo_grpc::Response<ScanResp>, ::volo_grpc::Status> {
        let v = ScanResp {
            values: todo!(),
            cache_hit: todo!(),
            query_time: todo!(),
        };
        todo!()
    }
}
