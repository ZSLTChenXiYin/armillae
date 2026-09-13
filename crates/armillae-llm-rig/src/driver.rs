//! Private boundary for one Rig call, before Rig's output-based normalization.
use armillae_core::FinishReason;
use futures_util::StreamExt;
use rig_core::{
    completion::{CompletionError, CompletionRequest, NormalizeCompletionResponse, Usage},
    http_client::HttpClientExt,
    providers::{anthropic, deepseek, minimax, moonshot, ollama, openai},
    streaming::RawStreamingResult,
};
use serde_json::{Map, Value};
use std::future::Future;

#[derive(Clone, Debug, Default)]
pub(crate) struct Terminal {
    pub id: Option<String>,
    pub model: Option<String>,
    pub finish_reason: Option<FinishReason>,
    pub usage: Usage,
    pub metadata: Map<String, Value>,
}

pub(crate) trait NativeResponse {
    fn normalize_native(
        self,
        provider: &str,
    ) -> Result<rig_core::completion::CompletionResponse, CompletionError>;
}
macro_rules! native_response {
    ($($response:path),*) => { $(impl NativeResponse for $response {
        fn normalize_native(self, provider:&str)->Result<rig_core::completion::CompletionResponse,CompletionError> { self.normalize(provider) }
    })* };
}
native_response!(
    openai::completion::CompletionResponse,
    deepseek::CompletionResponse,
    anthropic::completion::CompletionResponse
);
impl NativeResponse for ollama::CompletionResponse {
    fn normalize_native(
        self,
        _provider: &str,
    ) -> Result<rig_core::completion::CompletionResponse, CompletionError> {
        self.try_into()
    }
}

pub(crate) trait RigDriver: Send + Sync {
    type Response: NativeResponse + Send + Sync;
    fn completion(
        &self,
        request: CompletionRequest,
    ) -> impl Future<Output = Result<Self::Response, CompletionError>> + Send;
    fn stream(
        &self,
        request: CompletionRequest,
    ) -> impl Future<Output = Result<RawStreamingResult<Terminal>, CompletionError>> + Send;
}

trait IntoTerminal {
    fn into_terminal(self) -> Terminal;
}
impl<U> IntoTerminal for openai::StreamingCompletionResponse<U>
where
    for<'a> Usage: From<&'a U>,
{
    fn into_terminal(self) -> Terminal {
        let mut metadata = Map::new();
        if let Some(value) = self.additional_params
            && let Value::Object(value) = value.into_value()
        {
            metadata.extend(value);
        }
        if let Some(value) = self.logprobs {
            metadata.insert("logprobs".into(), value);
        }
        if let Some(value) = self.provider_request_id {
            metadata.insert("provider_request_id".into(), Value::String(value));
        }
        Terminal {
            id: self.response_id,
            model: self.model,
            finish_reason: self.finish_reason.map(|reason| match reason {
                rig_core::completion::FinishReason::Stop => FinishReason::Stop,
                rig_core::completion::FinishReason::Length => FinishReason::Length,
                rig_core::completion::FinishReason::ToolCalls => FinishReason::ToolCall,
                rig_core::completion::FinishReason::ContentFilter => FinishReason::ContentFilter,
                rig_core::completion::FinishReason::Other(value) => {
                    crate::response::openai_finish_reason(&value)
                }
            }),
            usage: Usage::from(&self.usage),
            metadata,
        }
    }
}
impl IntoTerminal for anthropic::streaming::StreamingCompletionResponse {
    fn into_terminal(self) -> Terminal {
        let mut metadata = Map::new();
        if let Some(value) = self.stop_sequence {
            metadata.insert("stop_sequence".into(), Value::String(value));
        }
        if let Some(value) = self.provider_request_id {
            metadata.insert("provider_request_id".into(), Value::String(value));
        }
        Terminal {
            id: self.message_id,
            model: self.model,
            finish_reason: self.stop_reason.as_deref().map(|r| match r {
                "end_turn" | "stop_sequence" => FinishReason::Stop,
                "tool_use" => FinishReason::ToolCall,
                "max_tokens" => FinishReason::Length,
                other => crate::response::openai_finish_reason(other),
            }),
            usage: Usage::from(&self.usage),
            metadata,
        }
    }
}
impl IntoTerminal for ollama::StreamingCompletionResponse {
    fn into_terminal(self) -> Terminal {
        let metadata = crate::providers::ollama::streaming_metadata(&self)
            .as_object()
            .cloned()
            .unwrap_or_default();
        Terminal {
            usage: Usage::from(&self),
            id: None,
            model: (!self.model.is_empty()).then_some(self.model),
            finish_reason: self
                .done_reason
                .as_deref()
                .map(crate::response::openai_finish_reason),
            metadata,
        }
    }
}

macro_rules! driver {
    ($model:path, $response:path) => {
        impl<H: HttpClientExt + Clone + Default + std::fmt::Debug + Send + Sync + 'static> RigDriver
            for $model
        {
            type Response = $response;
            async fn completion(
                &self,
                request: CompletionRequest,
            ) -> Result<Self::Response, CompletionError> {
                self.raw_completion(request).await
            }
            async fn stream(
                &self,
                request: CompletionRequest,
            ) -> Result<RawStreamingResult<Terminal>, CompletionError> {
                let stream = self.raw_stream(request).await?;
                Ok(Box::pin(stream.map(|item| {
                    item.and_then(|item| item.try_map_final(|r| Ok(r.into_terminal())))
                })))
            }
        }
    };
}
driver!(
    openai::completion::CompletionModel<H>,
    openai::completion::CompletionResponse
);
driver!(deepseek::CompletionModel<H>, deepseek::CompletionResponse);
driver!(openai::completion::GenericCompletionModel<minimax::MiniMaxExt, H>, openai::completion::CompletionResponse);
driver!(
    moonshot::CompletionModel<H>,
    openai::completion::CompletionResponse
);
driver!(
    anthropic::completion::CompletionModel<H>,
    anthropic::completion::CompletionResponse
);
driver!(ollama::CompletionModel<H>, ollama::CompletionResponse);
