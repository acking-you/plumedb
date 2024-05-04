use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use clap::Parser;
use colored::Colorize;
use futures::StreamExt;
use plumedb::common::config::init_tracing;
use mimalloc_rust::GlobalMiMalloc;
use nu_pretty_hex::PrettyHex;
use once_cell::sync::Lazy;
use pilota::Bytes;
use plumedb::cli::parser::{parse_common_op, BasicOp, ChannelOp, CommonOp, ShowItem};
use plumedb::cli::repl::get_repl;
use reedline::{DefaultPrompt, DefaultPromptSegment, Signal};
use volo::FastStr;
use volo_gen::plumedb::{
    ChannelsReq, ChannelsResp, DelReq, DelResp, FillReq, FillResp, GetReq, GetResp, KeyValePair,
    KeysReq, KeysResp, PlumeDbServiceClient, PlumeDbServiceClientBuilder, PublishReq, PublishResp,
    ScanReq, ScanResp, ShowReq, ShowResp, SubcribeReq, SubcribeResp,
};
use volo_grpc::{Request, Status};

#[global_allocator]
static GLOBAL_MIMALLOC: GlobalMiMalloc = GlobalMiMalloc;

const PLUME_CLIENT_ADDR_ENV: &str = "PLUME_SERVER_ADDR";

const DEFAULT_SERVER_ADDR: SocketAddr =
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

#[derive(Parser)]
#[command(author = "L_B__", version, about, long_about = None)]
struct Cli {
    /// Server address, by default it is `127.0.0.1:8080`
    #[arg(short, long, value_name = "PLUME_SERVER_ADDR")]
    server_addr: Option<String>,
}

static CLIENT: Lazy<Box<PlumeDbServiceClient>> = Lazy::new(|| {
    let addr = match std::env::var(PLUME_CLIENT_ADDR_ENV) {
        Ok(a) => match a.parse::<SocketAddr>() {
            Ok(addr) => addr,
            Err(e) => {
                tracing::error!(
                    "parse socket addr error!:{e}. we will use default server \
                     address:`{DEFAULT_SERVER_ADDR}`"
                );
                DEFAULT_SERVER_ADDR
            }
        },
        Err(_) => {
            tracing::info!("we use default server address:`{DEFAULT_SERVER_ADDR}` to build client");
            DEFAULT_SERVER_ADDR
        }
    };
    Box::new(
        PlumeDbServiceClientBuilder::new("plumedb")
            .address(addr)
            .build(),
    )
});

fn print_query_time_with_id(query_time: u64, query_id: &str) {
    println!(
        "Query Time: {:?} with query id: `{}`",
        Duration::from_nanos(query_time),
        query_id.yellow()
    )
}

fn print_string_or_hex_dump(bytes: &[u8]) {
    let hex_dump = format!("{:?}", bytes.hex_dump());
    let bytes = bytes.to_vec();
    match String::from_utf8(bytes) {
        Ok(o) => {
            println!("{o}");
        }
        Err(_) => {
            println!("Not valid utf8 string,we try to print hex-dump:");
            println!("{hex_dump}");
        }
    }
}

fn print_string_or_hex(bytes: &[u8]) {
    let hex_dump = format!("{}", bytes.hex_dump());
    let bytes = bytes.to_vec();
    match String::from_utf8(bytes) {
        Ok(o) => {
            println!("{o}");
        }
        Err(_) => {
            eprintln!("Not valid utf8 string,we try to print hex:");
            println!("{hex_dump}");
        }
    }
}

#[volo::main]
async fn main() {
    init_tracing::<true>();
    let resp = CLIENT
        .show(Request::new(ShowReq {
            status: volo_gen::plumedb::StatusType::Options,
        }))
        .await
        .unwrap();
    let ShowResp {
        status_graph,
        query_time,
        query_id,
    } = resp.into_inner();
    println!("\n Your LSM-Tree options:\n{status_graph}");
    print_query_time_with_id(query_time, &query_id);
    let cli = Cli::parse();
    if let Some(a) = cli.server_addr {
        std::env::set_var(PLUME_CLIENT_ADDR_ENV, a);
    }
    let mut line_editor = get_repl().expect("get repl never fails");
    let prompt = DefaultPrompt {
        left_prompt: DefaultPromptSegment::Basic("🤗✨🧨 >>".into()),
        right_prompt: DefaultPromptSegment::CurrentDateTime,
    };

    println!(
        r#"     __  ,
 .--()°'.' Welcome to {},
'|, . ,'   based on {},
 !_-(_\    where all data is structured!

    Our GitHub repository is at {}
    "#,
        "plumedb".green(),
        "lsm-kv".green(),
        "https://github.com/acking-you/plumedb".green()
    );

    fn print_exit_ok() {
        println!("\nSuccessfully exited the plumedb-repl. Have a great day! (oﾟvﾟ)ノ");
    }

    loop {
        let sig = line_editor.read_line(&prompt).unwrap();
        match sig {
            Signal::Success(buffer) => {
                if &buffer == "exit" {
                    print_exit_ok();
                    break;
                }
                if &buffer == "clear" {
                    let clear = crossterm::terminal::Clear(crossterm::terminal::ClearType::All);
                    println!("{clear}");
                    continue;
                }
                match parse_common_op(&buffer) {
                    Ok(o) => handle_command(o).await,
                    Err(e) => eprintln!("{e}"),
                }
            }
            Signal::CtrlD | Signal::CtrlC => {
                print_exit_ok();
                break;
            }
        }
    }
}

async fn handle_command(op: CommonOp) {
    match op {
        CommonOp::Basic(op) => handle_basic_op::<false>(op).await,
        CommonOp::Profile(op) => handle_basic_op::<true>(op).await,
        CommonOp::Show(op) => handle_show_op(op).await,
        CommonOp::Channel(op) => handle_channel_op(op).await,
    }
}

macro_rules! get_or_return {
    ($expr:expr) => {
        match $expr {
            Ok(o) => o,
            Err(e) => {
                eprintln!("{e}");
                return;
            }
        }
    };
}

async fn handle_channel_op(op: ChannelOp) {
    match op {
        ChannelOp::Show => {
            let ChannelsResp {
                query_time,
                query_id,
                channels,
                profiler,
            } = get_or_return!(
                CLIENT
                    .channels(Request::new(ChannelsReq {
                        channel: "".into(),
                        op: volo_gen::plumedb::ChannelOp::Show,
                        profiler: true,
                    }))
                    .await
            )
            .into_inner();
            println!("channels: {channels:?}\n");
            println!("channel show ok!");
            print_query_time_with_id(query_time, &query_id);
            if let Some(profiler) = profiler {
                println!("\nWith Profiler:\n{}\n", profiler.green());
            }
        }
        ChannelOp::Add(channel) => {
            let ChannelsResp {
                query_time,
                query_id,
                channels: _,
                profiler,
            } = get_or_return!(
                CLIENT
                    .channels(Request::new(ChannelsReq {
                        channel: channel.into(),
                        op: volo_gen::plumedb::ChannelOp::Add,
                        profiler: true,
                    }))
                    .await
            )
            .into_inner();
            println!("channel add ok!");
            print_query_time_with_id(query_time, &query_id);
            if let Some(profiler) = profiler {
                println!("\nWith Profiler:\n{}\n", profiler.green());
            }
        }
        ChannelOp::Publish(channel, values) => {
            let PublishResp {
                query_time,
                query_id,
                profiler,
            } = get_or_return!(
                CLIENT
                    .publish(Request::new(PublishReq {
                        channel: channel.into(),
                        value: values
                            .into_iter()
                            .map(|v| {
                                let fast_str: FastStr = v.into();
                                fast_str.into()
                            })
                            .collect(),
                        profiler: true,
                    }))
                    .await
            )
            .into_inner();
            println!("channel pub ok!");
            print_query_time_with_id(query_time, &query_id);
            if let Some(profiler) = profiler {
                println!("\nWith Profiler:\n{}\n", profiler.green());
            }
        }
        ChannelOp::Subcribe(need_prev_msg, channel) => {
            let mut subcribe_stream = get_or_return!(
                CLIENT
                    .subcribe(Request::new(SubcribeReq {
                        channel: channel.into(),
                        fetch_prev: need_prev_msg,
                    }))
                    .await
            )
            .into_inner();
            let signal = tokio::signal::ctrl_c();
            tokio::pin!(signal);

            loop {
                tokio::select! {
                    _ = &mut signal => {
                        tracing::info!("stopped subcribe!");
                        return;
                    },
                    ret = subcribe_stream.next() =>{
                        if let Some(ret) = ret{
                            handle_subcribee_ret(ret);
                        }else{
                            println!("\nchannel sub ok!");
                            return;
                        }
                    }
                }
            }
        }
    }
}

fn handle_subcribee_ret(ret: Result<SubcribeResp, Status>) {
    let SubcribeResp {
        pre_fetched_value,
        channal_value,
        query_time,
        query_id,
    } = get_or_return!(ret);
    if let Some(value) = pre_fetched_value {
        println!("Pre Fetched Value:");
        value.values.iter().for_each(|v| {
            print!(") ");
            print_string_or_hex(v);
        });
    }
    if let Some(value) = channal_value {
        println!("Channel Value Updated:");
        print!(") ");
        print_string_or_hex(&value);
    }
    print_query_time_with_id(query_time, &query_id);
}

async fn handle_show_op(op: ShowItem) {
    match op {
        ShowItem::MemTable => {
            let ShowResp {
                status_graph,
                query_time,
                query_id,
            } = get_or_return!(
                CLIENT
                    .show(Request::new(ShowReq {
                        status: volo_gen::plumedb::StatusType::Memtable
                    }))
                    .await
            )
            .into_inner();
            println!("{status_graph}\n");
            print_query_time_with_id(query_time, &query_id);
        }
        ShowItem::SSTable => {
            let ShowResp {
                status_graph,
                query_time,
                query_id,
            } = get_or_return!(
                CLIENT
                    .show(Request::new(ShowReq {
                        status: volo_gen::plumedb::StatusType::Sst
                    }))
                    .await
            )
            .into_inner();
            println!("{status_graph}\n");
            print_query_time_with_id(query_time, &query_id);
        }
        ShowItem::Level => {
            let ShowResp {
                status_graph,
                query_time,
                query_id,
            } = get_or_return!(
                CLIENT
                    .show(Request::new(ShowReq {
                        status: volo_gen::plumedb::StatusType::Level
                    }))
                    .await
            )
            .into_inner();
            println!("{status_graph}\n");
            print_query_time_with_id(query_time, &query_id);
        }
        ShowItem::Options => {
            let ShowResp {
                status_graph,
                query_time,
                query_id,
            } = get_or_return!(
                CLIENT
                    .show(Request::new(ShowReq {
                        status: volo_gen::plumedb::StatusType::Options
                    }))
                    .await
            )
            .into_inner();
            println!("{status_graph}\n");
            print_query_time_with_id(query_time, &query_id);
        }
        ShowItem::Files => {
            let ShowResp {
                status_graph,
                query_time,
                query_id,
            } = get_or_return!(
                CLIENT
                    .show(Request::new(ShowReq {
                        status: volo_gen::plumedb::StatusType::Files
                    }))
                    .await
            )
            .into_inner();
            println!("{status_graph}\n");
            print_query_time_with_id(query_time, &query_id);
        }
    }
}

async fn handle_basic_op<const NEED_PROFILER: bool>(op: BasicOp) {
    match op {
        BasicOp::Put(key, value) => {
            let FillResp {
                query_time,
                query_id,
                profiler,
            } = get_or_return!(
                CLIENT
                    .fill(Request::new(FillReq {
                        pairs: vec![KeyValePair {
                            key: Bytes::copy_from_slice(key.as_bytes()),
                            value: Bytes::copy_from_slice(value.as_bytes()),
                        }],
                        query_profiler: NEED_PROFILER,
                    }))
                    .await
            )
            .into_inner();
            println!("put ok!");
            print_query_time_with_id(query_time, &query_id);
            if let Some(profiler) = profiler {
                println!("\nWith Profiler:\n{}\n", profiler.green());
            }
        }
        BasicOp::Delete(key) => {
            let DelResp {
                query_time,
                query_id,
                profiler,
            } = get_or_return!(
                CLIENT
                    .delete(Request::new(DelReq {
                        query_profiler: NEED_PROFILER,
                        key: key.into()
                    }))
                    .await
            )
            .into_inner();
            println!("delete ok!");
            print_query_time_with_id(query_time, &query_id);
            if let Some(profiler) = profiler {
                println!("\nWith Profiler:\n{}\n", profiler.green());
            }
        }
        BasicOp::Get(key) => {
            let key: FastStr = key.into();
            let GetResp {
                query_time,
                query_id,
                profiler,
                value,
            } = get_or_return!(
                CLIENT
                    .get(Request::new(GetReq {
                        query_profiler: NEED_PROFILER,
                        key: key.clone().into()
                    }))
                    .await
            )
            .into_inner();
            match value {
                Some(value) => {
                    print_string_or_hex_dump(&value);
                }
                None => {
                    println!("`{key}` not exist!\n");
                }
            }
            println!("\nget ok!");
            print_query_time_with_id(query_time, &query_id);
            if let Some(profiler) = profiler {
                println!("\nWith Profiler:\n{}\n", profiler.green());
            }
        }
        BasicOp::Fill(op) => {
            let FillResp {
                query_time,
                query_id,
                profiler,
            } = get_or_return!(
                CLIENT
                    .fill(Request::new(FillReq {
                        query_profiler: NEED_PROFILER,
                        pairs: op
                            .into_iter()
                            .map(|(key, value)| KeyValePair {
                                key: key.into(),
                                value: value.into()
                            })
                            .collect()
                    }))
                    .await
            )
            .into_inner();
            println!("fill ok!");
            print_query_time_with_id(query_time, &query_id);
            if let Some(profiler) = profiler {
                println!("\nWith Profiler:\n{}\n", profiler.green());
            }
        }
        BasicOp::Scan(lower, upper) => {
            let ScanResp {
                query_time,
                query_id,
                profiler,
                pairs,
            } = get_or_return!(
                CLIENT
                    .scan(Request::new(ScanReq {
                        query_profiler: NEED_PROFILER,
                        lower: Some(lower.into_grpc_bound()),
                        upper: Some(upper.into_grpc_bound()),
                    }))
                    .await
            )
            .into_inner();
            pairs.iter().for_each(|pair| {
                print!("key: ");
                print_string_or_hex(&pair.key);
                print!("value: ");
                print_string_or_hex(&pair.value);
            });
            println!("\nscan ok!");
            print_query_time_with_id(query_time, &query_id);
            if let Some(profiler) = profiler {
                println!("\nWith Profiler:\n{}\n", profiler.green());
            }
        }
        BasicOp::Keys(lower, upper) => {
            let KeysResp {
                query_time,
                query_id,
                profiler,
                keys,
            } = get_or_return!(
                CLIENT
                    .keys(Request::new(KeysReq {
                        query_profiler: NEED_PROFILER,
                        lower: Some(lower.into_grpc_bound()),
                        upper: Some(upper.into_grpc_bound()),
                    }))
                    .await
            )
            .into_inner();
            keys.iter().for_each(|key| {
                print!("key: ");
                print_string_or_hex(key);
            });
            println!("\nkeys ok!");
            print_query_time_with_id(query_time, &query_id);
            if let Some(profiler) = profiler {
                println!("\nWith Profiler:\n{}\n", profiler.green());
            }
        }
    }
}
