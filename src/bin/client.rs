use std::net::SocketAddr;

use lsm_kv::common::config::init_tracing;
use volo_gen::plumedb::bound::Value;
use volo_gen::plumedb::{
    Bound, FillReq, GetReq, KeyValePair, KeysReq, LsmKvServiceClientBuilder, ShowReq, StatusType,
    Unbounded,
};

#[volo::main]
async fn main() {
    init_tracing::<false>();
    let client = LsmKvServiceClientBuilder::new("test-kv")
        .address("[::1]:8080".parse::<SocketAddr>().unwrap())
        .build();
    // let pairs = (1..1000000)
    //     .map(|i| KeyValePair {
    //         key: format!("{i}").into(),
    //         value: format!("{i}vv").into(),
    //     })
    //     .collect();
    let resp = client
        .get(GetReq {
            key: "abcd".into(),

            query_profiler: true,
        })
        .await
        .unwrap();
    let v = resp.into_inner();
    println!(
        "v{},resp:\n{}",
        String::from_utf8_lossy(&v.value.unwrap()),
        v.profiler.unwrap()
    );
}
