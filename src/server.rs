use std::pin::Pin;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, Result as FlightActionResult,
    SchemaResult, Ticket,
};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use tonic::{Request, Response, Status};

type TonicStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[derive(Debug, Clone)]
pub struct LanceFlyService {
    dataset_path: Option<String>,
}

impl LanceFlyService {
    pub fn new(dataset_path: Option<String>) -> Self {
        Self { dataset_path }
    }

    fn list_flights_once(&self) -> Vec<Result<FlightInfo, Status>> {
        let Some(dataset_path) = self.dataset_path.as_ref() else {
            return vec![];
        };

        let flight = FlightInfo {
            flight_descriptor: Some(FlightDescriptor {
                r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
                cmd: Bytes::new(),
                path: vec![dataset_path.clone()],
            }),
            ..Default::default()
        };
        vec![Ok(flight)]
    }
}

#[tonic::async_trait]
impl FlightService for LanceFlyService {
    type HandshakeStream = TonicStream<HandshakeResponse>;
    type ListFlightsStream = TonicStream<FlightInfo>;
    type DoGetStream = TonicStream<FlightData>;
    type DoPutStream = TonicStream<PutResult>;
    type DoExchangeStream = TonicStream<FlightData>;
    type DoActionStream = TonicStream<FlightActionResult>;
    type ListActionsStream = TonicStream<ActionType>;

    async fn handshake(
        &self,
        request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let mut in_stream = request.into_inner();
        let first = in_stream.next().await.transpose()?;
        let response = match first {
            Some(req) => HandshakeResponse {
                protocol_version: req.protocol_version,
                payload: req.payload,
            },
            None => HandshakeResponse::default(),
        };
        Ok(Response::new(Box::pin(tokio_stream::iter([Ok(response)]))))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(Box::pin(tokio_stream::iter(
            self.list_flights_once(),
        ))))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "GetFlightInfo is not implemented yet",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented(
            "PollFlightInfo is not implemented yet",
        ))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("GetSchema is not implemented yet"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("DoGet is not implemented yet"))
    }

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("DoPut is not implemented yet"))
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange is not implemented yet"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        let body = match action.r#type.as_str() {
            "health" => Bytes::from_static(b"ok"),
            "lance_info" => match self.dataset_path.as_ref() {
                Some(path) => Bytes::from(format!("dataset_path={path}")),
                None => Bytes::from_static(b"dataset_path="),
            },
            _ => {
                return Err(Status::invalid_argument(format!(
                    "unknown action type: {}",
                    action.r#type
                )));
            }
        };

        Ok(Response::new(Box::pin(tokio_stream::iter([Ok(
            FlightActionResult { body },
        )]))))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = [
            ActionType {
                r#type: "health".to_string(),
                description: "Return a single Flight action result with body 'ok'.".to_string(),
            },
            ActionType {
                r#type: "lance_info".to_string(),
                description: "Return the configured dataset path if present.".to_string(),
            },
        ];
        Ok(Response::new(Box::pin(tokio_stream::iter(
            actions.into_iter().map(Ok),
        ))))
    }
}
