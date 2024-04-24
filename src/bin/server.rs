use std::net::SocketAddr;

use plumedb::S;
use volo_grpc::server::{Server, ServiceBuilder};

#[volo::main]
async fn main() {
    let addr: SocketAddr = "[::]:8080".parse().unwrap();
    let addr = volo::net::Address::from(addr);

    Server::new()
        .add_service(
            ServiceBuilder::new(volo_gen::plumedb::HelloWorldServiceServer::new(S)).build(),
        )
        .run(addr)
        .await
        .unwrap();
}
