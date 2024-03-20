use volo_gen::plumedb::{HelloWorldReq, HelloWorldResp};
use volo_grpc::Response;

pub struct S;

impl volo_gen::plumedb::HelloWorldService for S {
    async fn hello_world(
        &self,
        req: ::volo_grpc::Request<HelloWorldReq>,
    ) -> Result<::volo_grpc::Response<HelloWorldResp>, ::volo_grpc::Status> {
        let req = req.into_inner();
        Ok(Response::new(HelloWorldResp {
            msg: format!("hello {}", req.name).into(),
        }))
    }
}
