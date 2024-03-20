use std::net::SocketAddr;

use volo_gen::plumedb::{HelloWorldReq, HelloWorldServiceClientBuilder};

#[volo::main]
async fn main() {
    let client = HelloWorldServiceClientBuilder::new("test-hello")
        .address("[::1]:8080".parse::<SocketAddr>().unwrap())
        .build();
    let resp = client
        .hello_world(HelloWorldReq { name: "LB".into() })
        .await
        .unwrap();
    let v = resp.into_inner();
    println!("resp:{}", v.msg);
}
