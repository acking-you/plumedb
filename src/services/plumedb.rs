use std::path::Path;

use lsm_kv::compact::CompactionOptions;
use lsm_kv::storage::lsm_storage::LsmStorageOptions;
use lsm_kv::storage::LsmKV;
use volo_gen::plumedb::{
    ChannelsReq, ChannelsResp, DelReq, DelResp, FillReq, FillResp, GetReq, GetResp, KeysReq,
    KeysResp, PublishReq, PublishResp, ScanReq, ScanResp, ShowReq, ShowResp, SubcribeReq,
    SubcribeResp,
};
use volo_grpc::{BoxStream, Response, Status};

use super::channel::{ChannelMap, MessageId, MessageIdBuilder};
use super::lsm_kv::LsmKVService;

pub struct PlumDBServiceImpl<T: CompactionOptions> {
    pub(crate) channel_id_builder: MessageIdBuilder,
    pub(crate) lsm_kv_service: LsmKVService<T>,
    pub(crate) channels: ChannelMap,
}

impl<T: CompactionOptions> PlumDBServiceImpl<T> {
    pub fn new(path: impl AsRef<Path>, options: LsmStorageOptions<T>) -> anyhow::Result<Self> {
        Ok(Self {
            lsm_kv_service: LsmKVService::new(LsmKV::open(path, options)?),
            channels: ChannelMap::new(),
            channel_id_builder: MessageIdBuilder::new(MessageId::default()),
        })
    }
}

impl<T: CompactionOptions> volo_gen::plumedb::PlumeDbService for PlumDBServiceImpl<T> {
    async fn fill(
        &self,
        req: ::volo_grpc::Request<FillReq>,
    ) -> ::std::result::Result<::volo_grpc::Response<FillResp>, Status> {
        self.fill_inner(req.into_inner())
    }

    async fn get(
        &self,
        req: ::volo_grpc::Request<GetReq>,
    ) -> std::result::Result<::volo_grpc::Response<GetResp>, ::volo_grpc::Status> {
        self.get_inner(req.into_inner())
    }

    async fn scan(
        &self,
        req: ::volo_grpc::Request<ScanReq>,
    ) -> std::result::Result<::volo_grpc::Response<ScanResp>, ::volo_grpc::Status> {
        self.scan_inner(req.into_inner())
    }

    async fn keys(
        &self,
        req: ::volo_grpc::Request<KeysReq>,
    ) -> Result<::volo_grpc::Response<KeysResp>, ::volo_grpc::Status> {
        self.keys_inner(req.into_inner())
    }

    async fn delete(
        &self,
        req: ::volo_grpc::Request<DelReq>,
    ) -> Result<::volo_grpc::Response<DelResp>, ::volo_grpc::Status> {
        self.delete_inner(req.into_inner())
    }

    async fn show(
        &self,
        req: ::volo_grpc::Request<ShowReq>,
    ) -> Result<::volo_grpc::Response<ShowResp>, ::volo_grpc::Status> {
        self.show_inner(req.into_inner())
    }

    async fn subcribe(
        &self,
        req: ::volo_grpc::Request<SubcribeReq>,
    ) -> Result<Response<BoxStream<'static, Result<SubcribeResp, Status>>>, Status> {
        self.subcribe_inner(req.into_inner()).await
    }

    async fn publish(
        &self,
        req: ::volo_grpc::Request<PublishReq>,
    ) -> Result<Response<PublishResp>, Status> {
        self.publish_inner(req.into_inner()).await
    }

    async fn channels(
        &self,
        req: ::volo_grpc::Request<ChannelsReq>,
    ) -> Result<Response<ChannelsResp>, Status> {
        self.channels_inner(req.into_inner()).await
    }
}
