use std::fmt::Debug;
use std::mem::size_of;
use std::ops::{Add, Bound};
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Context;
use dashmap::DashMap;
use lsm_kv::common::iterator::StorageIterator;
use lsm_kv::common::profier::{
    get_format_block_profiler, get_format_read_profiler, get_format_write_profiler, ReadProfiler,
    Timer, WriteProfiler,
};
use lsm_kv::compact::CompactionOptions;
use lsm_kv::storage::lsm_storage::{LsmStorageOptions, WriteBatchRecord};
use lsm_kv::storage::LsmKV;
use pilota::{Buf, BufMut, Bytes};
use tokio_stream::wrappers::ReceiverStream;
use uuid::Uuid;
use volo::FastStr;
use volo_gen::plumedb::{
    ChannelsReq, ChannelsResp, FetchedValue, PublishReq, PublishResp, SubcribeReq, SubcribeResp,
};
use volo_grpc::{BoxStream, Response, Status};

use super::lsm_kv::LsmKVService;
use super::plumedb::PlumDBServiceImpl;

pub type ChannelMap = DashMap<FastStr, (MessageIdBuilder, Vec<(Uuid, flume::Sender<Bytes>)>)>;

/// Represents channel message id
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct MessageId(usize);

impl Debug for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Add<usize> for MessageId {
    type Output = Self;

    fn add(self, rhs: usize) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl MessageId {
    /// MSG_ID is fixed and stored as 8bytes.
    fn encode(&self, vec: &mut Vec<u8>) {
        vec.put_u64(self.0 as u64)
    }

    const fn raw_len() -> usize {
        size_of::<u64>()
    }

    fn to_bytes(self) -> Bytes {
        let mut bytes = Vec::with_capacity(Self::raw_len());
        bytes.put_u64(self.0 as u64);
        bytes.into()
    }
}

impl From<usize> for MessageId {
    #[inline]
    fn from(value: usize) -> Self {
        Self(value)
    }
}

impl From<u64> for MessageId {
    #[inline]
    fn from(value: u64) -> Self {
        Self(value as usize)
    }
}

impl From<u32> for MessageId {
    #[inline]
    fn from(value: u32) -> Self {
        Self(value as usize)
    }
}

impl From<MessageId> for usize {
    #[inline]
    fn from(value: MessageId) -> Self {
        value.0
    }
}

#[derive(Default, Debug, Clone)]
pub struct MessageIdBuilder(Arc<AtomicUsize>);

impl MessageIdBuilder {
    pub fn new(init_id: MessageId) -> Self {
        Self(AtomicUsize::new(init_id.0).into())
    }

    pub fn next_id(&self) -> MessageId {
        self.0
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            .into()
    }

    pub fn cur_id(&self) -> MessageId {
        self.0.load(std::sync::atomic::Ordering::SeqCst).into()
    }
}

/// Store all messages uniformly in the `m_{CHANNEL_NAME}{MSG_ID}` format
pub const CHAN_MSG_PREFIX: &[u8] = b"m_";
/// Store all channel name uniformly in the `n_{CHANNEL_ID}` format
pub const CHAN_NAME_PREFIX: &[u8] = b"n_";
/// Store the latest msg_id of the channel in the format `i_{CHANNEL_NAME}`
pub const CHAN_MSG_ID_PREFIX: &[u8] = b"i_";
/// Store the latest chan_id of the channel in the key "c_id"
pub const CHAN_ID_KEY: &[u8] = b"c_id";

fn get_chan_msg_key(channel_name: &str, msg_id: MessageId) -> Bytes {
    let channel_name = channel_name.as_bytes();
    let mut key =
        Vec::with_capacity(CHAN_MSG_PREFIX.len() + channel_name.len() + MessageId::raw_len());
    key.put_slice(CHAN_MSG_PREFIX);
    key.put_slice(channel_name);
    msg_id.encode(&mut key);
    key.into()
}

fn get_chan_latest_msg_id_key(channel_name: &str) -> Bytes {
    let channel_name = channel_name.as_bytes();
    let mut key = Vec::with_capacity(CHAN_MSG_ID_PREFIX.len() + channel_name.len());
    key.put_slice(CHAN_MSG_ID_PREFIX);
    key.put_slice(channel_name);
    key.into()
}

fn get_chan_msg_bound(channel_name: &str) -> (Bytes, Bytes) {
    let channel_name = channel_name.as_bytes();
    let prefix = CHAN_MSG_PREFIX
        .iter()
        .chain(channel_name.iter())
        .copied()
        .collect::<Vec<_>>();
    let (mut lower, mut upper) = (prefix.clone(), prefix.clone());
    lower.put_u64(u64::MIN);
    upper.put_u64(u64::MAX);
    (lower.into(), upper.into())
}

fn get_chan_name_key(chan_id: MessageId) -> Bytes {
    let mut key = Vec::with_capacity(CHAN_NAME_PREFIX.len() + MessageId::raw_len());
    key.put_slice(CHAN_NAME_PREFIX);
    chan_id.encode(&mut key);
    key.into()
}

const fn get_chan_name_key_bound() -> (Bound<&'static [u8]>, Bound<&'static [u8]>) {
    const LOWER: [u8; CHAN_NAME_PREFIX.len() + MessageId::raw_len()] = [
        CHAN_NAME_PREFIX[0],
        CHAN_NAME_PREFIX[1],
        u8::MIN,
        u8::MIN,
        u8::MIN,
        u8::MIN,
        u8::MIN,
        u8::MIN,
        u8::MIN,
        u8::MIN,
    ];
    const UPPER: [u8; CHAN_NAME_PREFIX.len() + size_of::<u64>()] = [
        CHAN_NAME_PREFIX[0],
        CHAN_NAME_PREFIX[1],
        u8::MAX,
        u8::MAX,
        u8::MAX,
        u8::MAX,
        u8::MAX,
        u8::MAX,
        u8::MAX,
        u8::MAX,
    ];
    (Bound::Included(&LOWER), Bound::Excluded(&UPPER))
}

impl<T: CompactionOptions> PlumDBServiceImpl<T> {
    pub fn recover(path: impl AsRef<Path>, options: LsmStorageOptions<T>) -> anyhow::Result<Self> {
        let lsm_kv_service = LsmKVService::new(LsmKV::open(path, options)?);
        // Scans out pre-existing channels
        let (lower, upper) = get_chan_name_key_bound();
        let mut iter = lsm_kv_service
            .0
            .scan(lower, upper)
            .context("Recover when scan error")?;
        if !iter.is_valid() {
            tracing::info!("No pub/sub operations have been performed previously");
            Ok(Self {
                lsm_kv_service,
                channels: ChannelMap::new(),
                channel_id_builder: MessageIdBuilder::default(),
            })
        } else {
            let mut msg_id_read = ReadProfiler::default();
            let channels = ChannelMap::new();
            while iter.is_valid() {
                let raw_name = iter.value();
                let channel_name = FastStr::from_bytes(Bytes::copy_from_slice(raw_name))
                    .context("[channel name] bytes to string error")?;
                let key = get_chan_latest_msg_id_key(&channel_name);
                let id = match lsm_kv_service
                    .0
                    .get_with_profier(&mut msg_id_read, &key)
                    .context("[channel msg id] get latest channel msg id error")?
                {
                    Some(mut b) => b.get_u64(),
                    None => 0,
                };

                channels.insert(channel_name, (MessageIdBuilder::new(id.into()), Vec::new()));
                iter.next().context("[channel name] iter next error")?;
            }
            let id = lsm_kv_service
                .0
                .get_with_profier(&mut msg_id_read, CHAN_ID_KEY)
                .context("[channel id builder] get error")?
                .context("[channel id builder] chan_id not exist")?
                .get_u64();
            let channel_id_builder = MessageIdBuilder::new(id.into());
            let channel_name_read = iter.block_profiler();
            tracing::info!(
                "Recover pub/sub ok! read msg_id profiler:\n{}\nread channel name profiler:\n{}",
                get_format_read_profiler(&msg_id_read),
                get_format_block_profiler(&channel_name_read)
            );
            Ok(Self {
                lsm_kv_service,
                channels,
                channel_id_builder,
            })
        }
    }

    pub async fn subcribe_inner(
        &self,
        req: SubcribeReq,
    ) -> Result<Response<BoxStream<'static, Result<SubcribeResp, Status>>>, Status> {
        let raw_uuid = uuid::Uuid::new_v4();
        let uuid: FastStr = raw_uuid.to_string().into();
        let span = tracing::info_span!("UUID", uuid = uuid.as_str());
        let _enter = span.enter();
        let key = req.channel.clone();
        const CHAN_CAP: usize = 1024 * 4;
        let timer = Timer::now();
        let (msg_tx, msg_rx) = flume::bounded(CHAN_CAP);
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(CHAN_CAP);
        let register_tx = msg_tx.clone();
        let is_channel_exist = match self.channels.entry(req.channel) {
            dashmap::mapref::entry::Entry::Occupied(mut o) => {
                o.get_mut().1.push((raw_uuid, register_tx));
                true
            }
            dashmap::mapref::entry::Entry::Vacant(v) => {
                let builder = MessageIdBuilder::new(MessageId(0));
                v.insert((builder.clone(), vec![(raw_uuid, register_tx)]));
                false
            }
        };
        // register channel
        if !is_channel_exist {
            let mut write_profiler = WriteProfiler::default();
            self.register_channel(key.clone(), &mut write_profiler)
                .await?;
            let profiler = get_format_write_profiler(&write_profiler);
            tracing::info!("register channel profiler:\n{}", profiler);
        }
        // send prev msg
        if is_channel_exist && req.fetch_prev {
            let (lower, upper) = get_chan_msg_bound(&key);
            let mut iter = self
                .lsm_kv_service
                .0
                .scan(Bound::Included(&lower), Bound::Excluded(&upper))
                .with_context(|| format!("Scan previous message error with channal:{key}"))?;
            let mut pre_fetched_value = FetchedValue { values: vec![] };
            while iter.is_valid() {
                pre_fetched_value
                    .values
                    .push(Bytes::copy_from_slice(iter.value()));
                iter.next().context("[subcribe pre_fetched] iter next")?;
            }
            resp_tx
                .send(Ok(SubcribeResp {
                    pre_fetched_value: Some(pre_fetched_value),
                    channal_value: None,
                    query_time: timer.elapsed().as_nanos() as u64,
                    query_id: uuid.clone(),
                }))
                .await
                .context("[subcribe pre_fetched] send error")?;
            let profiler = iter.block_profiler();
            tracing::info!(
                "[subcribe pre_fetched] profiler:\n{}",
                get_format_block_profiler(&profiler)
            );
        }
        // waiting for new msg
        tokio::spawn(async move {
            let span = tracing::info_span!("UUID", uuid = uuid.as_str());
            let _enter = span.enter();
            loop {
                let timer = Timer::now();
                let msg = msg_rx.recv_async().await;

                match msg {
                    Ok(v) => {
                        if let Err(e) = resp_tx
                            .send(Ok(SubcribeResp {
                                pre_fetched_value: None,
                                channal_value: Some(v),
                                query_time: timer.elapsed().as_nanos() as u64,
                                query_id: uuid.clone(),
                            }))
                            .await
                            .context("[subcribe] send error")
                        {
                            tracing::error!("subcriber send fails:{e}");
                            return;
                        }
                    }
                    Err(e) => {
                        tracing::error!("subcriber recv fails:{e}");
                        return;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(resp_rx))))
    }

    async fn register_channel(
        &self,
        channel: FastStr,
        write_profiler: &mut WriteProfiler,
    ) -> anyhow::Result<()> {
        let id = self.channel_id_builder.next_id();
        let key = get_chan_name_key(id);
        let value = channel.clone().into_bytes();
        self.lsm_kv_service
            .0
            .write_bytes_batch_with_profier(write_profiler, &[WriteBatchRecord::Put(key, value)])
            .context("[register channel name]")?;
        let cur_id = self.channel_id_builder.cur_id();
        if cur_id == id + 1 {
            self.lsm_kv_service
                .0
                .write_bytes_batch_with_profier(
                    write_profiler,
                    &[WriteBatchRecord::Put(CHAN_ID_KEY.into(), cur_id.to_bytes())],
                )
                .context("[register channel latest id]")?;
        }
        tracing::info!("register channel name:{} ok!", channel);
        Ok(())
    }

    pub async fn publish_inner(&self, req: PublishReq) -> Result<Response<PublishResp>, Status> {
        let uuid: FastStr = uuid::Uuid::new_v4().to_string().into();
        let span = tracing::info_span!("UUID", uuid = uuid.as_str());
        let _enter = span.enter();
        let PublishReq {
            channel,
            value,
            profiler,
        } = req;
        let (msg_id_builder, channels, is_channel_exist) =
            match self.channels.entry(channel.clone()) {
                dashmap::mapref::entry::Entry::Occupied(o) => {
                    let (builder, channels) = o.get();
                    (builder.clone(), channels.clone(), true)
                }
                dashmap::mapref::entry::Entry::Vacant(v) => {
                    let builder = MessageIdBuilder::new(MessageId(0));
                    v.insert((builder.clone(), Vec::new()));
                    (builder, Vec::new(), false)
                }
            };

        let mut write_profiler = WriteProfiler::default();

        // register channel name when it is first publish
        if !is_channel_exist {
            self.register_channel(channel.clone(), &mut write_profiler)
                .await?;
        }

        // write values to KV store
        let mut msg_id: MessageId;
        msg_id = msg_id_builder.next_id();
        let write_batch = value
            .iter()
            .map(|v| {
                msg_id = msg_id_builder.next_id();
                let key = get_chan_msg_key(&channel, msg_id);
                WriteBatchRecord::Put(key, v.clone())
            })
            .collect::<Vec<_>>();
        self.lsm_kv_service
            .0
            .write_bytes_batch_with_profier(&mut write_profiler, &write_batch)
            .context("[publish KV Store]")?;

        // update values to subcriber
        for (uuid, sender) in channels.iter() {
            for v in value.iter() {
                if let Err(e) = sender.send_async(v.clone()).await {
                    tracing::error!(
                        "[publish: send value] send message value to channel:{uuid} with error:{e}"
                    );
                    break;
                }
            }
        }

        // update latest channel msg id
        let cur_id = msg_id_builder.cur_id();
        if msg_id + 1 == cur_id {
            let msg_id_key = get_chan_latest_msg_id_key(&channel);
            self.lsm_kv_service
                .0
                .write_bytes_batch_with_profier(
                    &mut write_profiler,
                    &[WriteBatchRecord::Put(msg_id_key, cur_id.to_bytes())],
                )
                .context("[publish KV msg id]")?;
        }
        let profiler_text = get_format_write_profiler(&write_profiler)
            .to_string()
            .into();
        tracing::info!("publish ok,with write profiler:\n{profiler_text}");

        Ok(Response::new(PublishResp {
            query_time: write_profiler.write_total_time.as_nanos() as u64,
            query_id: uuid,
            profiler: if profiler { Some(profiler_text) } else { None },
        }))
    }

    pub async fn channels_inner(&self, req: ChannelsReq) -> Result<Response<ChannelsResp>, Status> {
        let uuid: FastStr = uuid::Uuid::new_v4().to_string().into();
        let span = tracing::info_span!("UUID", uuid = uuid.as_str());
        let _enter = span.enter();
        let ChannelsReq {
            channel,
            op,
            profiler,
        } = req;
        let time = Timer::now();
        let (channels, profiler) = match op {
            volo_gen::plumedb::ChannelOp::Add => {
                if self.channels.get(&channel).is_none() {
                    let builder = MessageIdBuilder::new(MessageId(0));
                    self.channels.insert(channel.clone(), (builder, Vec::new()));
                    let mut write_profiler = WriteProfiler::default();
                    self.register_channel(channel, &mut write_profiler).await?;
                    let table = get_format_write_profiler(&write_profiler);
                    tracing::info!("[channel add] profiler:\n{table}");
                    (
                        Vec::new(),
                        if profiler {
                            Some(table.to_string().into())
                        } else {
                            None
                        },
                    )
                } else {
                    (Vec::new(), None)
                }
            }
            volo_gen::plumedb::ChannelOp::Show => {
                let (lower, upper) = get_chan_name_key_bound();
                let mut iter = self
                    .lsm_kv_service
                    .0
                    .scan(lower, upper)
                    .context("[channel show] scan")?;
                let mut channels = Vec::new();
                while iter.is_valid() {
                    channels.push(
                        // SAFETY: this `[u8]` must be encoded with utf8
                        unsafe { FastStr::from_vec_u8_unchecked(iter.value().into()) },
                    );
                    iter.next().context("[channel show] iter next")?;
                }
                let block_profiler = iter.block_profiler();
                let table = get_format_block_profiler(&block_profiler);
                tracing::info!("[channel show] profiler:\n{table}");
                (
                    channels,
                    if profiler {
                        Some(table.to_string().into())
                    } else {
                        None
                    },
                )
            }
        };
        Ok(Response::new(ChannelsResp {
            query_time: time.elapsed().as_nanos() as u64,
            query_id: uuid,
            profiler,
            channels,
        }))
    }
}
