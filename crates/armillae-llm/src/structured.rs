use std::fmt;

use armillae_core::{
    AssistantContent, CompletionEvent, CompletionResponse, FinishReason, OutputFormat,
};
use futures_util::{StreamExt, stream};
use jsonschema::{Draft, Retrieve, Uri, Validator};
use serde_json::Value;

use crate::{BridgeError, CompletionStream, ErrorMetadata, StructuredOutputErrorKind};

/// A compiled final-response check shared by Drivers and mocks.
///
/// Preparation performs no I/O. Legacy output formats retain their existing semantics.
/// Neither Debug nor errors expose schema documents or generated content.
pub struct OutputValidation {
    validator: Option<Validator>,
}

impl fmt::Debug for OutputValidation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutputValidation")
            .field("enabled", &self.validator.is_some())
            .finish_non_exhaustive()
    }
}

struct NoRetrieval;

impl Retrieve for NoRetrieval {
    fn retrieve(&self, _: &Uri<String>) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        Err("external schema retrieval is disabled".into())
    }
}

impl OutputValidation {
    /// Compile before invoking a model. Unknown dialects and external resources fail closed.
    pub fn prepare(format: Option<&OutputFormat>) -> Result<Self, BridgeError> {
        let Some(OutputFormat::Structured { name, schema, .. }) = format else {
            return Ok(Self { validator: None });
        };
        if name.trim().is_empty() || !schema.is_object() {
            return Err(BridgeError::InvalidOutputSchema);
        }
        let draft = match schema.get("$schema") {
            None => Draft::Draft202012,
            Some(Value::String(uri)) => match uri.trim_end_matches('#') {
                "http://json-schema.org/draft-04/schema" => Draft::Draft4,
                "http://json-schema.org/draft-06/schema" => Draft::Draft6,
                "http://json-schema.org/draft-07/schema" => Draft::Draft7,
                "https://json-schema.org/draft/2019-09/schema" => Draft::Draft201909,
                "https://json-schema.org/draft/2020-12/schema" => Draft::Draft202012,
                _ => return Err(BridgeError::InvalidOutputSchema),
            },
            _ => return Err(BridgeError::InvalidOutputSchema),
        };
        let validator = jsonschema::options()
            .with_draft(draft)
            .with_retriever(NoRetrieval)
            .should_validate_formats(false)
            .build(schema)
            .map_err(|_| BridgeError::InvalidOutputSchema)?;
        Ok(Self {
            validator: Some(validator),
        })
    }

    /// Check a complete response without changing content, order, IDs, usage or metadata.
    pub fn validate(&self, response: &CompletionResponse) -> Result<(), BridgeError> {
        let Some(validator) = &self.validator else {
            return Ok(());
        };
        if response
            .content
            .iter()
            .any(|part| matches!(part, AssistantContent::ToolCall(_)))
        {
            return failure(StructuredOutputErrorKind::ToolCall);
        }
        if !matches!(response.finish_reason, None | Some(FinishReason::Stop)) {
            return failure(StructuredOutputErrorKind::Incomplete);
        }
        let mut text = String::new();
        for part in &response.content {
            match part {
                AssistantContent::Text(part) => text.push_str(&part.text),
                AssistantContent::ProviderData(_) => {}
                _ => return failure(StructuredOutputErrorKind::Incomplete),
            }
        }
        if text.trim().is_empty() {
            return failure(StructuredOutputErrorKind::MissingText);
        }
        let value: Value =
            serde_json::from_str(&text).map_err(|_| BridgeError::StructuredOutput {
                kind: StructuredOutputErrorKind::InvalidJson,
            })?;
        if !value.is_object() {
            return failure(StructuredOutputErrorKind::NotObject);
        }
        if !validator.is_valid(&value) {
            return failure(StructuredOutputErrorKind::SchemaMismatch);
        }
        Ok(())
    }

    /// Forward unvalidated preview events; gate the sole successful terminal event.
    /// The underlying stream is dropped on completion, failure, or consumer cancellation.
    pub fn stream(self, inner: CompletionStream, provider: impl Into<String>) -> CompletionStream {
        if self.validator.is_none() {
            return inner;
        }
        let provider = provider.into();
        Box::pin(stream::unfold(
            Some((inner, self, provider)),
            |state| async move {
                let (mut inner, validation, provider) = state?;
                match inner.next().await {
                    Some(Ok(CompletionEvent::ResponseCompleted { response })) => {
                        let item = validation
                            .validate(&response)
                            .map(|()| CompletionEvent::ResponseCompleted { response });
                        Some((item, None))
                    }
                    Some(Err(error)) => Some((Err(error), None)),
                    Some(Ok(event)) => Some((Ok(event), Some((inner, validation, provider)))),
                    None => Some((
                        Err(BridgeError::StreamInterrupted {
                            metadata: ErrorMetadata::new(provider),
                        }),
                        None,
                    )),
                }
            },
        ))
    }
}

fn failure(kind: StructuredOutputErrorKind) -> Result<(), BridgeError> {
    Err(BridgeError::StructuredOutput { kind })
}
