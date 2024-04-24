use volo_gen::plumedb::{
    FillReq, FillResp, GetReq, GetResp, HelloWorldReq, HelloWorldResp, ScanReq, ScanResp,
};
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

impl volo_gen::plumedb::LsmKvService for S {
    async fn fill(
        &self,
        req: ::volo_grpc::Request<FillReq>,
    ) -> ::std::result::Result<::volo_grpc::Response<FillResp>, ::volo_grpc::Status> {
        todo!()
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
