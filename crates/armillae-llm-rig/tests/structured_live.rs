//! Explicit Provider × mode × transport Live gates. No automatic mode selection.
//! Set ARMILLAE_LIVE_<PROFILE>_MODEL and optionally ARMILLAE_LIVE_<PROFILE>_ENDPOINT.
//! Credentials use <PROFILE>_API_KEY; OPENAI_COMPATIBLE requires an endpoint.
//! Run only with the host's authorization; every test is ignored by default.

use armillae_core::{CompletionRequest, Message, OutputFormat, StructuredOutputMode};
use armillae_llm::{
    BridgeConfig, BridgeError, BridgeFactory, CredentialRef, mock::contract::validate_stream_events,
};
use armillae_llm_rig::RigBridgeFactory;
use futures::StreamExt;
use serde_json::json;
use std::{env, error::Error, io};

async fn run(
    provider: &str,
    profile: &str,
    mode: StructuredOutputMode,
    streaming: bool,
    supported: bool,
) -> Result<(), Box<dyn Error>> {
    let model = env::var(format!("ARMILLAE_LIVE_{profile}_MODEL"))
        .map_err(|_| io::Error::other("explicit profile model is required for this Live gate"))?;
    let mut builder = BridgeConfig::builder(provider, model);
    if provider != "ollama" {
        builder = builder.credential(CredentialRef::Environment {
            name: format!("{profile}_API_KEY"),
        });
    }
    if let Ok(endpoint) = env::var(format!("ARMILLAE_LIVE_{profile}_ENDPOINT")) {
        builder = builder.endpoint(endpoint.parse()?);
    }
    let bridge = RigBridgeFactory
        .create(builder.build()?.resolve().await?)
        .await?;
    let request = CompletionRequest {
        messages: vec![Message::user(
            "Return only a JSON object with one field answer containing the string armillae.",
        )],
        output_format: Some(OutputFormat::Structured {
            name: "answer".into(),
            mode,
            schema: json!({"type":"object","properties":{"answer":{"type":"string","enum":["armillae"]}},"required":["answer"],"additionalProperties":false}),
        }),
        generation: armillae_core::GenerationOptions {
            max_output_tokens: Some(4096),
            ..Default::default()
        },
        ..Default::default()
    };
    if !supported {
        assert!(matches!(
            bridge.project(&request),
            Err(BridgeError::UnsupportedCapability { .. })
        ));
        if streaming {
            assert!(matches!(
                bridge.stream(request).await,
                Err(BridgeError::UnsupportedCapability { .. })
            ));
        } else {
            assert!(matches!(
                bridge.complete(request).await,
                Err(BridgeError::UnsupportedCapability { .. })
            ));
        }
        return Ok(());
    }
    bridge.project(&request)?;
    if streaming {
        let events = bridge
            .stream(request)
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        validate_stream_events(&events)?;
    } else {
        bridge.complete(request).await?;
    }
    Ok(())
}

macro_rules! gates {
    ($module:ident,$provider:literal,$profile:literal,$native:literal,$client:literal) => {
        mod $module {
            use super::*;
            #[tokio::test(flavor = "current_thread")]
            #[ignore = "requires explicit model, credential and host authorization"]
            async fn native_complete() -> Result<(), Box<dyn Error>> {
                run(
                    $provider,
                    $profile,
                    StructuredOutputMode::NativeStrict,
                    false,
                    $native,
                )
                .await
            }
            #[tokio::test(flavor = "current_thread")]
            #[ignore = "requires explicit model, credential and host authorization"]
            async fn native_stream() -> Result<(), Box<dyn Error>> {
                run(
                    $provider,
                    $profile,
                    StructuredOutputMode::NativeStrict,
                    true,
                    $native,
                )
                .await
            }
            #[tokio::test(flavor = "current_thread")]
            #[ignore = "requires explicit model, credential and host authorization"]
            async fn client_complete() -> Result<(), Box<dyn Error>> {
                run(
                    $provider,
                    $profile,
                    StructuredOutputMode::JsonObjectValidated,
                    false,
                    $client,
                )
                .await
            }
            #[tokio::test(flavor = "current_thread")]
            #[ignore = "requires explicit model, credential and host authorization"]
            async fn client_stream() -> Result<(), Box<dyn Error>> {
                run(
                    $provider,
                    $profile,
                    StructuredOutputMode::JsonObjectValidated,
                    true,
                    $client,
                )
                .await
            }
        }
    };
}
gates!(openai, "openai", "OPENAI", true, true);
gates!(
    openai_compatible,
    "openai-compatible",
    "OPENAI_COMPATIBLE",
    true,
    true
);
gates!(deepseek, "deepseek", "DEEPSEEK", false, true);
gates!(minimax, "minimax", "MINIMAX", false, true);
gates!(moonshot, "moonshot", "MOONSHOT", false, true);
gates!(anthropic, "anthropic", "ANTHROPIC", true, false);
gates!(ollama, "ollama", "OLLAMA", true, true);
