use std::net::SocketAddr;

use lsm_kv::common::config::init_tracing;
use lsm_kv::compact::leveled::LeveledCompactionOptions;
use lsm_kv::storage::lsm_storage::LsmStorageOptions;
use lsm_kv::storage::LsmKV;
use plumedb::common::config::get_lsm_path;
use plumedb::services::lsm_kv::LsmKVServiceImpl;
use volo_grpc::server::{Server, ServiceBuilder};

#[volo::main]
async fn main() {
    init_tracing::<false>();
    let addr: SocketAddr = "[::]:8080".parse().unwrap();
    let addr = volo::net::Address::from(addr);
    let compaction_options = LeveledCompactionOptions {
        level_size_multiplier: 4,
        level0_file_num_compaction_trigger: 2,
        max_levels: 2,
        base_level_size_bytes: 2 << 20, // 2MB
    };
    let lsm_storage_options = LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 1 << 20, // 1MB
        compaction_options,
        enable_wal: true,
        num_memtable_limit: 2,
    };

    let storage = LsmKV::open(get_lsm_path().unwrap(), lsm_storage_options).unwrap();
    let s = LsmKVServiceImpl::new(storage);
    Server::new()
        .add_service(ServiceBuilder::new(volo_gen::plumedb::LsmKvServiceServer::new(s)).build())
        .run(addr)
        .await
        .unwrap();
}
