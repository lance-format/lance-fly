use std::net::SocketAddr;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Action;
use bytes::Bytes;
use lance_fly::server::LanceFlyService;
use tokio::net::TcpListener;
use tokio::time::{sleep, Duration};
use tonic::transport::Channel;

async fn connect_with_retry(addr: SocketAddr) -> FlightServiceClient<tonic::transport::Channel> {
    let endpoint = format!("http://{addr}");
    for _ in 0..50 {
        match Channel::from_shared(endpoint.clone())
            .expect("endpoint")
            .connect()
            .await
        {
            Ok(channel) => return FlightServiceClient::new(channel),
            Err(_) => sleep(Duration::from_millis(20)).await,
        }
    }
    let channel = Channel::from_shared(endpoint)
        .expect("endpoint")
        .connect()
        .await
        .expect("connect Flight server");
    FlightServiceClient::new(channel)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flight_health_action_works() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    let svc =
        arrow_flight::flight_service_server::FlightServiceServer::new(LanceFlyService::new(None));

    let handle = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(incoming)
            .await
            .expect("serve");
    });

    let mut client = connect_with_retry(addr).await;
    let action = Action {
        r#type: "health".to_string(),
        body: Bytes::new(),
    };

    let mut stream = client
        .do_action(action)
        .await
        .expect("do_action")
        .into_inner();
    let resp = stream.message().await.expect("read").expect("message");
    assert_eq!(resp.body, Bytes::from_static(b"ok"));

    handle.abort();
}
