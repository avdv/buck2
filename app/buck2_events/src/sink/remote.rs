/*
 * This source code is licensed under both the MIT license found in the
 * LICENSE-MIT file in the root directory of this source tree and the Apache
 * License, Version 2.0 found in the LICENSE-APACHE file in the root directory
 * of this source tree.
 */

//! A Sink for forwarding events directly to Remote service.
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use fbinit::FacebookInit;

#[cfg(fbcode_build)]
mod fbcode {
    pub use scribe_client::ScribeConfig;

    pub use crate::sink::scribe::RemoteEventSink;
    pub(crate) use crate::sink::scribe::scribe_category;
}

#[cfg(not(fbcode_build))]
mod fbcode {
    use std::collections::HashMap;
    use std::env::VarError;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::thread::JoinHandle;
    use std::time::Duration;

    use allocative::Allocative;
    use anyhow::Context;
    use async_stream::stream;
    use bes_proto::google::devtools::build::v1;
    use bes_proto::google::devtools::build::v1::OrderedBuildEvent;
    use bes_proto::google::devtools::build::v1::PublishBuildToolEventStreamRequest;
    use bes_proto::google::devtools::build::v1::StreamId;
    use bes_proto::google::devtools::build::v1::publish_build_event_client::PublishBuildEventClient;
    use buck2_data;
    use buck2_data::BuildCommandStart;
    use buck2_error::ErrorTag;
    use buck2_error::conversion::from_any_with_tag;
    use buck2_util::future::try_join_all;
    use dupe::Dupe;
    use futures::Stream;
    use futures::StreamExt;
    use futures::stream;
    use once_cell::sync::Lazy;
    use prost;
    use prost::Message;
    use prost_types;
    use regex::Regex;
    use tokio::runtime::Builder;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::sync::mpsc::UnboundedSender;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tonic::Request;
    use tonic::metadata;
    use tonic::metadata::MetadataKey;
    use tonic::metadata::MetadataValue;
    use tonic::service::Interceptor;
    use tonic::service::interceptor::InterceptedService;
    use tonic::transport::Channel;
    use tonic::transport::channel::ClientTlsConfig;

    use crate::BuckEvent;
    use crate::Event;
    use crate::EventSink;
    use crate::EventSinkStats;
    use crate::EventSinkWithStats;

    pub struct RemoteEventSink {
        _handler: JoinHandle<()>,
        send: UnboundedSender<Vec<BuckEvent>>,
    }

    // TODO[AH] re-use definitions from REOSS crate.
    #[derive(Clone, Debug, Default, Allocative)]
    pub struct HttpHeader {
        pub key: String,
        pub value: String,
    }

    impl FromStr for HttpHeader {
        type Err = anyhow::Error;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let mut iter = s.splitn(2, ':');
            match (iter.next(), iter.next()) {
                (Some(key), Some(value)) => Ok(Self {
                    key: key.trim().to_owned(),
                    value: value.trim().to_owned(),
                }),
                _ => Err(anyhow::anyhow!(
                    "Invalid header (expect name and value separated by `:`): `{}`",
                    s
                )),
            }
        }
    }

    /// Replace occurrences of $FOO in a string with the value of the env var $FOO.
    fn substitute_env_vars(s: &str) -> anyhow::Result<String> {
        substitute_env_vars_impl(s, |v| std::env::var(v))
    }

    fn substitute_env_vars_impl(
        s: &str,
        getter: impl Fn(&str) -> Result<String, VarError>,
    ) -> anyhow::Result<String> {
        static ENV_REGEX: Lazy<Regex> =
            Lazy::new(|| Regex::new("\\$[a-zA-Z_][a-zA-Z_0-9]*").unwrap());

        let mut out = String::with_capacity(s.len());
        let mut last_idx = 0;

        for mat in ENV_REGEX.find_iter(s) {
            out.push_str(&s[last_idx..mat.start()]);
            let var = &mat.as_str()[1..];
            let val =
                getter(var).with_context(|| format!("Error substituting `{}`", mat.as_str()))?;
            out.push_str(&val);
            last_idx = mat.end();
        }

        if last_idx < s.len() {
            out.push_str(&s[last_idx..s.len()]);
        }

        Ok(out)
    }

    #[derive(Clone, Dupe)]
    struct InjectHeadersInterceptor {
        headers: Arc<Vec<(MetadataKey<metadata::Ascii>, MetadataValue<metadata::Ascii>)>>,
    }

    impl InjectHeadersInterceptor {
        pub fn new(headers: &[HttpHeader]) -> anyhow::Result<Self> {
            let headers = headers
                .iter()
                .map(|h| {
                    // This means we can't have `$` in a header key or value, which isn't great. On the
                    // flip side, env vars are good for things like credentials, which those headers
                    // are likely to contain. In time, we should allow escaping.
                    let key = substitute_env_vars(&h.key)?;
                    let value = substitute_env_vars(&h.value)?;

                    let key = MetadataKey::<metadata::Ascii>::from_bytes(key.as_bytes())
                        .with_context(|| format!("Invalid key in header: `{}: {}`", key, value))?;

                    let value = MetadataValue::try_from(&value).with_context(|| {
                        format!("Invalid value in header: `{}: {}`", key, value)
                    })?;

                    anyhow::Ok((key, value))
                })
                .collect::<Result<_, _>>()
                .context("Error converting headers")?;

            Ok(Self {
                headers: Arc::new(headers),
            })
        }
    }

    impl Interceptor for InjectHeadersInterceptor {
        fn call(
            &mut self,
            mut request: tonic::Request<()>,
        ) -> Result<tonic::Request<()>, tonic::Status> {
            for (k, v) in self.headers.iter() {
                request.metadata_mut().insert(k.clone(), v.clone());
            }
            Ok(request)
        }
    }

    type GrpcService = InterceptedService<Channel, InjectHeadersInterceptor>;

    async fn connect_build_event_server()
    -> buck2_error::Result<PublishBuildEventClient<GrpcService>> {
        let uri = std::env::var("BES_URI")
            .map_err(|e| from_any_with_tag(e, ErrorTag::Tier0))?
            .parse()?;
        let mut channel = Channel::builder(uri);
        let tls_config = ClientTlsConfig::new();
        {
            let tls_setting = std::env::var("BES_TLS").unwrap_or("0".to_owned());
            match tls_setting.as_str() {
                "1" | "true" => {
                    channel = channel.tls_config(tls_config)?;
                }
                _ => {}
            }
        }
        // TODO: parse PEM
        let endpoint = channel
            .connect()
            .await
            .context("connecting to Bazel event stream gRPC server")
            .map_err(|e| from_any_with_tag(e, ErrorTag::Tier0))?;
        let mut headers = vec![];
        for hdr in std::env::var("BES_HEADERS")
            .unwrap_or("".to_owned())
            .split(",")
        {
            let hdr = hdr.trim();
            if !hdr.is_empty() {
                headers.push(
                    HttpHeader::from_str(hdr).map_err(|e| from_any_with_tag(e, ErrorTag::Tier0))?,
                );
            }
        }
        let interceptor = InjectHeadersInterceptor::new(&headers)
            .map_err(|e| from_any_with_tag(e, ErrorTag::Tier0))?;
        let client = PublishBuildEventClient::with_interceptor(endpoint, interceptor);
        Ok(client)
    }

    fn buck_to_bazel_events<S: Stream<Item = BuckEvent>>(
        events: S,
    ) -> impl Stream<Item = v1::BuildEvent> {
        stream! {
            for await event in events {
                let build_tool_event = v1::build_event::Event::BuildToolEvent(prost_types::Any {
                    // package + message from app/buck2_data/data.proto
                    type_url: "type.googleapis.com/buck.data.BuckEvent".to_owned(),
                    // The original prost message bytes
                    value: event.event.encode_to_vec(),
                });
                yield v1::BuildEvent {
                    event_time: Some(event.timestamp().into()),
                    event: Some(build_tool_event),
                };
            }
        }
    }

    fn stream_build_tool_events<S: Stream<Item = v1::BuildEvent>>(
        trace_id: String,
        events: S,
    ) -> impl Stream<Item = PublishBuildToolEventStreamRequest> {
        stream::iter(1..)
            .zip(events)
            .map(move |(sequence_number, event)| {
                PublishBuildToolEventStreamRequest {
                    check_preceding_lifecycle_events_present: false,
                    notification_keywords: vec![],
                    ordered_build_event: Some(OrderedBuildEvent {
                        stream_id: Some(StreamId {
                            build_id: trace_id.clone(),
                            invocation_id: trace_id.clone(),
                            component: 0,
                        }),
                        sequence_number,
                        event: Some(event),
                    }),
                    project_id: "12341234".to_owned(), // TODO: needed
                }
            })
    }

    async fn event_sink_loop(recv: UnboundedReceiver<Vec<BuckEvent>>) -> anyhow::Result<()> {
        let mut handlers: HashMap<
            String,
            (
                UnboundedSender<BuckEvent>,
                tokio::task::JoinHandle<anyhow::Result<()>>,
            ),
        > = HashMap::new();
        let client = connect_build_event_server().await?;
        let mut recv = UnboundedReceiverStream::new(recv).flat_map(|v| stream::iter(v));
        let result_uri = std::env::var("BES_RESULT").ok();
        while let Some(event) = recv.next().await {
            //let dbg_trace_id = event.event.trace_id.clone();
            //println!("event_sink_loop event {:?}", &dbg_trace_id);
            if let Some((send, _)) = handlers.get(&event.event.trace_id) {
                //println!("event_sink_loop redirect {:?}", &dbg_trace_id);
                send.send(event).unwrap_or_else(|e| {
                    let evt = &e.0;
                    eprintln!("error: sending event {evt:?} failed: {e:#?}")
                    // TODO at the end fails with SendError, ie. receiver end is already closed
                });
            } else {
                eprintln!("new channel");
                //println!("event_sink_loop new handler {:?}", event.event.trace_id);
                let (send, recv) = mpsc::unbounded_channel::<BuckEvent>();
                let mut client = client.clone();
                let result_uri = result_uri.clone();
                //let dbg_trace_id = dbg_trace_id.clone();
                let trace_id = event.event.trace_id.clone();
                let handler = tokio::spawn(async move {
                    let recv = UnboundedReceiverStream::new(recv);
                    let request = Request::new(stream_build_tool_events(
                        trace_id.clone(),
                        buck_to_bazel_events(recv),
                    ));
                    if let Some(result_uri) = result_uri.as_ref() {
                        eprintln!("BES results: {}{}", &result_uri, &trace_id);
                    }
                    //println!("BES request {:?}", &dbg_trace_id);
                    let response = match client.publish_build_tool_event_stream(request).await {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("BES stream error (publish_build_tool_event_stream): {e:#?}");
                            return Err(anyhow::anyhow!(e));
                        }
                    };
                    //println!("BES response {:?}", &dbg_trace_id);
                    let mut inbound = response.into_inner();
                    loop {
                        match inbound.message().await {
                            Ok(Some(ack)) => {
                                // TODO: Handle ACKs properly and add retry.
                                eprintln!("ACK  {:?} (not handled)", ack);
                            }
                            Ok(None) => break,
                            Err(e) => {
                                eprintln!("BES inbound error: {e:#?}");
                                break;
                            }
                        }
                    }
                    if let Some(result_uri) = result_uri.as_ref() {
                        println!("BES results: {}{}", &result_uri, &trace_id);
                    }
                    Ok(())
                });
                handlers.insert(event.event.trace_id.to_owned(), (send, handler));
            }
        }
        //println!("event_sink_loop recv CLOSED");
        // TODO: handle closure and retry.
        // close send handles and await all handlers.
        let handlers: Vec<tokio::task::JoinHandle<anyhow::Result<()>>> =
            handlers.into_values().map(|(_, handler)| handler).collect();
        // TODO: handle retry.
        match try_join_all(handlers).await {
            Ok(results) => {
                if let Err(e) = results.into_iter().collect::<anyhow::Result<Vec<()>>>() {
                    eprintln!("BES handler error: {e:#?}");
                    return Err(e);
                }
            }
            Err(e) => {
                eprintln!("BES join_all error: {e:#?}");
                return Err(anyhow::anyhow!(e));
            }
        }
        Ok(())
    }

    impl RemoteEventSink {
        pub async fn send_now(&self, event: BuckEvent) -> buck2_error::Result<()> {
            self.send_messages_now(vec![event]).await
        }
        pub async fn send_messages_now(&self, events: Vec<BuckEvent>) -> buck2_error::Result<()> {
            // TODO: does this make sense for BES? If so, implement send now variant.
            if let Err(err) = self.send.send(events) {
                // TODO: proper error handling
                dbg!(err);
            }
            Ok(())
        }
        pub fn new() -> buck2_error::Result<Self> {
            let (send, recv) = mpsc::unbounded_channel::<Vec<BuckEvent>>();
            let handler = std::thread::Builder::new()
                .name("buck-event-producer".to_owned())
                .spawn({
                    move || {
                        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
                        runtime.block_on(event_sink_loop(recv)).unwrap();
                    }
                })
                .context("spawning buck-event-producer thread")
                .map_err(|e| from_any_with_tag(e, ErrorTag::Tier0))?;
            Ok(RemoteEventSink {
                _handler: handler,
                send,
            })
        }
        pub fn offer(&self, event: BuckEvent) {
            if let Err(err) = self.send.send(vec![event]) {
                // TODO: proper error handling
                dbg!(err);
            }
        }
    }

    impl EventSink for RemoteEventSink {
        fn send(&self, event: Event) {
            match event {
                Event::Buck(event) => {
                    self.offer(event);
                }
                Event::CommandResult(..) => {}
                Event::PartialResult(..) => {}
            }
        }
    }

    impl EventSinkWithStats for RemoteEventSink {
        fn to_event_sync(self: Arc<Self>) -> Arc<dyn EventSink> {
            self as _
        }

        fn stats(&self) -> EventSinkStats {
            EventSinkStats {
                successes: 0,
                failures_invalid_request: 0,
                failures_unauthorized: 0,
                failures_rate_limited: 0,
                failures_pushed_back: 0,
                failures_enqueue_failed: 0,
                failures_internal_error: 0,
                failures_timed_out: 0,
                failures_unknown: 0,
                buffered: 0,
                dropped: 0,
                bytes_written: 0,
            }
        }
    }

    #[derive(Default)]
    pub struct ScribeConfig {
        pub buffer_size: usize,
        pub retry_backoff: Duration,
        pub retry_attempts: usize,
        pub message_batch_size: Option<usize>,
        pub thrift_timeout: Duration,
    }
}

pub use fbcode::*;

fn new_remote_event_sink_if_fbcode(
    fb: FacebookInit,
    config: ScribeConfig,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    #[cfg(fbcode_build)]
    {
        Ok(Some(RemoteEventSink::new(fb, scribe_category()?, config)?))
    }
    #[cfg(not(fbcode_build))]
    {
        let _ = (fb, config);
        match std::env::var("BES_URI") {
            Ok(_) => Ok(Some(RemoteEventSink::new()?)),
            _ => Ok(None),
        }
    }
}

pub fn new_remote_event_sink_if_enabled(
    fb: FacebookInit,
    config: ScribeConfig,
) -> buck2_error::Result<Option<RemoteEventSink>> {
    if is_enabled() {
        new_remote_event_sink_if_fbcode(fb, config)
    } else {
        Ok(None)
    }
}

/// Whether or not remote event logging is enabled for this process. It must be explicitly disabled via `disable()`.
static REMOTE_EVENT_SINK_ENABLED: AtomicBool = AtomicBool::new(true);

/// Returns whether this process should actually write to remote sink, even if it is fully supported by the platform and
/// binary.
pub fn is_enabled() -> bool {
    REMOTE_EVENT_SINK_ENABLED.load(Ordering::Relaxed)
}

/// Disables remote event logging for this process. Remote event logging must be disabled explicitly on startup, otherwise it is
/// on by default.
pub fn disable() {
    REMOTE_EVENT_SINK_ENABLED.store(false, Ordering::Relaxed);
}
