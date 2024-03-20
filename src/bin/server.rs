use std::net::SocketAddr;

use volo_grpc::server::{Server, ServiceBuilder};

use plumedb::S;

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
