use std::ops::Bound;
use std::sync::Arc;

use anyhow::Context;
use lsm_kv::common::iterator::StorageIterator;
use lsm_kv::common::profier::{ReadProfiler, Timer, WriteProfiler};
use lsm_kv::compact::CompactionOptions;
use lsm_kv::storage::lsm_iterator::{FusedIterator, LsmIterator};
use lsm_kv::storage::lsm_storage::WriteBatchRecord;
use lsm_kv::storage::LsmKV;
use pilota::Bytes;
use tabled::Table;
use uuid::Uuid;
use volo_gen::plumedb::bound::Value;
use volo_gen::plumedb::{
    Bound as RpcBound, DelReq, DelResp, FillReq, FillResp, GetReq, GetResp, KeyValePair, KeysReq,
    KeysResp, ScanReq, ScanResp, ShowReq, ShowResp,
};
use volo_grpc::{Response, Status};

macro_rules! required_not_empty {
    ($field_name:expr, $option:expr) => {
        match $option {
            Some(v) => v,
            None => {
                tracing::error!("field:{} must not be empty!", $field_name);
                return Err(Status::invalid_argument(format!(
                    "field:{} must not be empty!",
                    $field_name
                )));
            }
        }
    };
}

fn map_rpc_bound(value: &Value) -> Bound<&[u8]> {
    match value {
        Value::Unbouned(_) => Bound::Unbounded,
        Value::Include(v) => Bound::Included(v),
        Value::Exclude(v) => Bound::Excluded(v),
    }
}

pub struct LsmKVServiceImpl<T: CompactionOptions>(Arc<LsmKV<T>>);

impl<T: CompactionOptions> LsmKVServiceImpl<T> {
    pub fn new(storage: Arc<LsmKV<T>>) -> Self {
        Self(storage)
    }

    fn fill_inner(&self, pairs: Vec<KeyValePair>) -> anyhow::Result<WriteProfiler> {
        let write_batch = pairs
            .into_iter()
            .map(|pair| {
                let KeyValePair { key, value } = pair;
                if value.is_empty() {
                    WriteBatchRecord::Del(key)
                } else {
                    WriteBatchRecord::Put(key, value)
                }
            })
            .collect::<Vec<_>>();
        let mut profiler = WriteProfiler::default();
        self.0
            .write_bytes_batch_with_profier(&mut profiler, &write_batch)?;
        Ok(profiler)
    }

    fn get_inner(&self, key: Bytes) -> anyhow::Result<(Option<Bytes>, ReadProfiler)> {
        let mut profiler = ReadProfiler::default();
        let res = self.0.get_with_profier(&mut profiler, &key)?;
        Ok((res, profiler))
    }

    fn scan_inner(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> anyhow::Result<FusedIterator<LsmIterator>> {
        let res = self.0.scan(lower, upper)?;
        Ok(res)
    }
}

impl<T: CompactionOptions> volo_gen::plumedb::LsmKvService for LsmKVServiceImpl<T> {
    async fn fill(
        &self,
        req: ::volo_grpc::Request<FillReq>,
    ) -> ::std::result::Result<::volo_grpc::Response<FillResp>, Status> {
        let FillReq {
            pairs,
            query_profiler,
        } = req.into_inner();

        let query_id = Uuid::new_v4().to_string();
        let uuid_span = tracing::info_span!("UUID", query_id);
        let _enter = uuid_span.enter();
        let profiler = self.fill_inner(pairs).map_err(|e| {
            tracing::error!("fill error:{e}");
            Status::internal(format!("{e}"))
        })?;

        let query_time = profiler.write_total_time.as_nanos() as u64;
        let profiler = Table::new([&profiler]).to_string();
        tracing::info!("filled with profiler:\n{}", profiler);

        let resp = FillResp {
            query_time,
            query_id: query_id.into(),
            profiler: if query_profiler {
                Some(profiler.into())
            } else {
                None
            },
        };
        Ok(Response::new(resp))
    }

    async fn get(
        &self,
        req: ::volo_grpc::Request<GetReq>,
    ) -> std::result::Result<::volo_grpc::Response<GetResp>, ::volo_grpc::Status> {
        let GetReq {
            key,
            query_profiler,
        } = req.into_inner();

        let query_id = Uuid::new_v4().to_string();
        let uuid_span = tracing::info_span!("UUID", query_id);
        let _enter = uuid_span.enter();
        let (value, profiler) = self.get_inner(key).map_err(|e| {
            tracing::error!("get error:{e}");
            Status::internal(format!("{e}"))
        })?;

        let query_time = profiler.read_total_time.as_nanos() as u64;
        let profiler = Table::new([&profiler]).to_string();
        tracing::info!("get with profiler:\n{}", profiler);

        Ok(Response::new(GetResp {
            value,
            query_time,
            query_id: query_id.into(),
            profiler: if query_profiler {
                Some(profiler.into())
            } else {
                None
            },
        }))
    }

    async fn scan(
        &self,
        req: ::volo_grpc::Request<ScanReq>,
    ) -> std::result::Result<::volo_grpc::Response<ScanResp>, ::volo_grpc::Status> {
        let ScanReq {
            upper,
            lower,
            query_profiler,
        } = req.into_inner();

        let query_id = Uuid::new_v4().to_string();
        let uuid_span = tracing::info_span!("UUID", query_id);
        let _enter = uuid_span.enter();
        let RpcBound { value } = required_not_empty!("scan::upper", upper);
        let upper = required_not_empty!("scan::upper-value", value);
        let RpcBound { value } = required_not_empty!("scan::lower", lower);
        let lower = required_not_empty!("scan::lower-value", value);

        let total_time = Timer::now();
        let mut iter = self
            .scan_inner(map_rpc_bound(&lower), map_rpc_bound(&upper))
            .map_err(|e| Status::internal(format!("{e}")))?;

        let mut pairs = Vec::new();
        while iter.is_valid() {
            pairs.push(KeyValePair {
                key: Bytes::copy_from_slice(iter.key()),
                value: Bytes::copy_from_slice(iter.value()),
            });
            iter.next().map_err(|e| Status::internal(format!("{e}")))?;
        }
        let profiler = iter.block_profiler();
        let profiler = Table::new([&profiler]).to_string();

        tracing::info!("scan with profiler:\n{}", profiler);
        Ok(Response::new(ScanResp {
            pairs,
            query_time: total_time.elapsed().as_nanos() as u64,
            query_id: query_id.into(),
            profiler: if query_profiler {
                Some(profiler.into())
            } else {
                None
            },
        }))
    }

    async fn keys(
        &self,
        req: ::volo_grpc::Request<KeysReq>,
    ) -> Result<::volo_grpc::Response<KeysResp>, ::volo_grpc::Status> {
        let KeysReq {
            upper,
            lower,
            query_profiler,
        } = req.into_inner();

        let query_id = Uuid::new_v4().to_string();
        let uuid_span = tracing::info_span!("UUID", query_id);
        let _enter = uuid_span.enter();
        let RpcBound { value } = required_not_empty!("keys::upper", upper);
        let upper = required_not_empty!("keys::upper-value", value);
        let RpcBound { value } = required_not_empty!("keys::lower", lower);
        let lower = required_not_empty!("keys::lower-value", value);

        let total_time = Timer::now();
        let mut iter = self
            .scan_inner(map_rpc_bound(&lower), map_rpc_bound(&upper))
            .map_err(|e| Status::internal(format!("{e}")))?;

        let mut keys = Vec::new();
        while iter.is_valid() {
            keys.push(Bytes::copy_from_slice(iter.key()));
            iter.next().map_err(|e| Status::internal(format!("{e}")))?;
        }
        let profiler = iter.block_profiler();
        let profiler = Table::new([&profiler]).to_string();
        tracing::info!("keys with profiler:\n{}", profiler);
        Ok(Response::new(KeysResp {
            keys,
            query_time: total_time.elapsed().as_nanos() as u64,
            query_id: query_id.into(),
            profiler: if query_profiler {
                Some(profiler.into())
            } else {
                None
            },
        }))
    }

    async fn delete(
        &self,
        req: ::volo_grpc::Request<DelReq>,
    ) -> Result<::volo_grpc::Response<DelResp>, ::volo_grpc::Status> {
        let DelReq {
            key,
            query_profiler,
        } = req.into_inner();

        let mut profiler = WriteProfiler::default();
        let query_id = Uuid::new_v4().to_string();
        let uuid_span = tracing::info_span!("UUID", query_id);
        let _enter = uuid_span.enter();
        self.0
            .write_bytes_batch_with_profier(&mut profiler, &[WriteBatchRecord::Del(key)])
            .map_err(|e| Status::internal(format!("{e}")))?;
        let query_time = profiler.write_total_time.as_nanos() as u64;
        let profiler = Table::new([&profiler]).to_string();

        tracing::info!("delete with profiler:\n{}", profiler);
        Ok(Response::new(DelResp {
            query_time,
            query_id: query_id.into(),
            profiler: if query_profiler {
                Some(profiler.into())
            } else {
                None
            },
        }))
    }

    async fn show(
        &self,
        req: ::volo_grpc::Request<ShowReq>,
    ) -> Result<::volo_grpc::Response<ShowResp>, ::volo_grpc::Status> {
        let ShowReq { status } = req.into_inner();

        let query_id = Uuid::new_v4().to_string();
        let uuid_span = tracing::info_span!("UUID", query_id);
        let _enter = uuid_span.enter();
        let total_time = Timer::now();
        let status_graph = match status {
            volo_gen::plumedb::StatusType::Files => self
                .0
                .show_files()
                .map_err(|e| Status::internal(format!("show files status err:{e}")))?,
            volo_gen::plumedb::StatusType::Memtable => self.0.show_mem_status(),
            volo_gen::plumedb::StatusType::Sst => self.0.show_sst_status(),
            volo_gen::plumedb::StatusType::Level => self.0.show_level_status(),
            volo_gen::plumedb::StatusType::Options => self.0.show_options(),
        };
        Ok(Response::new(ShowResp {
            status_graph: status_graph.into(),
            query_time: total_time.elapsed().as_nanos() as u64,
            query_id: query_id.into(),
        }))
    }
}
