//! some config options

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer;

pub fn init_tracing<const NO_FORMAT: bool>() {
    if NO_FORMAT {
        no_format_tracing()
    } else {
        format_tracing()
    }
}

fn no_format_tracing() {
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .with_level(false)
            .with_file(false)
            .with_target(false)
            .without_time()
            .with_writer(std::io::stdout)
            .with_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                    .from_env_lossy(),
            ),
    );
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");
}

fn format_tracing() {
    let subscriber = tracing_subscriber::registry().with(
        tracing_subscriber::fmt::layer()
            .pretty()
            .with_writer(std::io::stdout)
            .with_filter(
                tracing_subscriber::EnvFilter::builder()
                    .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                    .from_env_lossy(),
            ),
    );
    tracing::subscriber::set_global_default(subscriber).expect("setting tracing default failed");
}
