use std::net::SocketAddr;

use once_cell::sync::Lazy;
use pilota::Bytes;
use volo_gen::plumedb::{
    FillReq, GetReq, GetResp, KeyValePair, PlumeDbServiceClient, PlumeDbServiceClientBuilder,
};
use volo_grpc::Request;

static CLIENT: Lazy<PlumeDbServiceClient> = Lazy::new(|| {
    let addr = "127.0.0.1:8080".parse::<SocketAddr>().unwrap();
    PlumeDbServiceClientBuilder::new("plumedb")
        .address(addr)
        .build()
});

#[tokio::test]
async fn test_fill_large_and_get_middle() {
    let mut pairs = Vec::new();
    let start = 10_000_000;
    let end = 20_000_000;
    for i in start..end {
        pairs.push(KeyValePair {
            key: format!("key:{i}").into(),
            value: format!("value:{i}").into(),
        });
    }
    let req = FillReq {
        pairs,
        query_profiler: true,
    };

    let resp = CLIENT.fill(Request::new(req)).await.unwrap().into_inner();
    println!("{}", resp.profiler.unwrap());

    let middle = (start + end) / 2;
    let GetResp {
        value,
        query_time: _,
        query_id: _,
        profiler,
    } = CLIENT
        .get(Request::new(GetReq {
            key: format!("key:{}", middle).into(),
            query_profiler: true,
        }))
        .await
        .unwrap()
        .into_inner();
    println!("{}", profiler.unwrap());
    let expected_value: Bytes = format!("value:{}", middle).into();
    assert_eq!(value.unwrap(), expected_value);
}
