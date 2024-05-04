use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use lsm_kv::common::config::init_tracing;
use lsm_kv::compact::leveled::LeveledCompactionOptions;
use lsm_kv::compact::CompactionOptions;
use lsm_kv::storage::lsm_storage::LsmStorageOptions;
use mimalloc_rust::GlobalMiMalloc;
use plumedb::common::config::{get_lsm_path, CompactionOptions as CliCompactionOptions};
use plumedb::services::plumedb::PlumDBServiceImpl;
use volo_grpc::server::{Server, ServiceBuilder};

#[global_allocator]
static GLOBAL_MIMALLOC: GlobalMiMalloc = GlobalMiMalloc;

#[derive(Parser)]
#[command(author = "L_B__", version, about, long_about = None)]
struct Cli {
    /// Configuration of different compaction strategies
    #[command(subcommand)]
    compaction_options: CliCompactionOptions,
    /// Server address, by default it is `[::]:8080`
    #[arg(short, long)]
    server_addr: Option<String>,
    /// Folder path for the entire LSM-Tree store. By default it is `~/.lsm`
    #[arg(short, long)]
    lsm_path: Option<PathBuf>,
    /// Block size in bytes. By default it is 16KB
    #[arg(short, long, default_value_t = 16 * 1024)]
    block_size: usize,
    /// SST size in bytes, also the approximate memtable capacity limit. By default it is 4MB
    #[arg(short, long, default_value_t = 4<<20)]
    target_sst_size: usize,
    /// Enable or disable WAL, if not, MemTable can be persisted only on normal close. By default
    /// it is enable
    #[arg(short, long, default_value_t = true)]
    enable_wal: bool,
    /// Maximum number of memtables in memory, flush to L0 when exceeding this limit. By default it
    /// is 3
    #[arg(short, long, default_value_t = 3)]
    num_memtable_limit: usize,
}

#[volo::main]
async fn main() {
    let cli = Cli::parse();

    init_tracing::<false>();

    match cli.compaction_options {
        CliCompactionOptions::Leveled {
            level_size_multiplier,
            level0_file_num_compaction_trigger,
            max_levels,
            base_level_size_bytes,
        } => {
            let compaction_options = LeveledCompactionOptions {
                level_size_multiplier,
                level0_file_num_compaction_trigger,
                max_levels,
                base_level_size_bytes,
            };
            run_with_compaction_options(compaction_options, &cli).await
        }
        _ => panic!("not implement"),
    }
}

async fn run_with_compaction_options<T: CompactionOptions>(compaction_options: T, cli: &Cli) {
    let lsm_storage_options = LsmStorageOptions {
        block_size: cli.block_size,
        target_sst_size: cli.target_sst_size,
        compaction_options,
        enable_wal: cli.enable_wal,
        num_memtable_limit: cli.num_memtable_limit,
    };

    let addr: SocketAddr = match &cli.server_addr {
        Some(a) => a.parse().unwrap(),
        None => "[::]:8080".parse().unwrap(),
    };
    let addr = volo::net::Address::from(addr);

    let s = PlumDBServiceImpl::recover(get_lsm_path().unwrap(), lsm_storage_options).unwrap();
    Server::new()
        .add_service(ServiceBuilder::new(volo_gen::plumedb::PlumeDbServiceServer::new(s)).build())
        .run(addr)
        .await
        .unwrap();
}
