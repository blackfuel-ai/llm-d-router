# Predicted Latency Producer (`predicted-latency-producer`)

**Type:** `predicted-latency-producer`

Trains XGBoost models via a sidecar and generates per-endpoint TTFT/TPOT predictions.

## Interfaces

DataProducer, PreRequest, ResponseHeader, ResponseBody, ProducerPlugin, ConsumerPlugin

## Responsibilities

- Bulk predictions during `Produce` (writes `LatencyPredictionInfo` to endpoint attributes)
- SLO headroom calculation per endpoint: `headroom = SLO - predicted_latency` (used by downstream scorer and admission plugins)
- TTFT and TPOT training data collection at end of stream, labelled from the
  target endpoint's latency windows (see Training labels below)
- Per-endpoint running request queue tracking (TPOT SLO priority queue)
- Prefix cache score forwarding from `PrefixCacheMatchInfo` attributes
- Multimodal encoder-cache size forwarding from `EncoderCacheMatchInfo` attributes
  (opt-in via `useEncoderCacheFeatures`)
- TPOT neutralization for prefill endpoints in disaggregated serving

## Config

| Parameter | Default | Description |
|-----------|---------|-------------|
| `latencyWindowPluginRef` | required | Name of the `vllm-latency-window-extractor` instance whose TTFT and TPOT windows label the training samples |
| `sloBufferFactor` | `1.0` | Multiplier for SLO headroom calculation |
| `contextTTL` | `5m` | TTL for per-request context in the cache |
| `endpointRoleLabel` | `""` | Label key for disaggregated serving roles |
| `predictInProduce` | `true` | Enable/disable bulk predictions. Set false for training-only mode |
| `useEncoderCacheFeatures` | `false` | Feed multimodal encoder-cache sizes (`encoder_input_size`, `encoder_matched_size`) to the predictor. Requires (and auto-creates) a multimodal encoder-cache producer |
| `encoderCacheMatchInfoProducerName` | `""` | Multimodal encoder-cache producer to read match data from. Empty defaults to the auto-created producer |

`samplingMean` and `maxDecodeTokenSamplesForPrediction` are accepted and
ignored (deprecated).

## Training labels

Both labels of a completed request come from the target endpoint's latency
windows at end of stream, read from the extractor instance named above:

- **TTFT** is the window quantile of `vllm:time_to_first_token_seconds`.
- **TPOT** is the window quantile of `vllm:inter_token_latency_seconds`.
  Under continuous batching every running request advances one token per
  decode step, so the endpoint's inter-token latency is the TPOT of every
  request it serves.

The response mode is irrelevant: a response streamed in chunks and a response
that arrives whole yield the same samples, so TPOT is trained and predicted for
every request. A window that never published, is empty, or is older than ten
seconds contributes no sample for that label; the other label is still sent.
Features (KV cache, queue, in-flight load, prefix score) are the values
captured when the request was scheduled.

The extractor must be attached to the pool's `metrics-data-source`; see the
extractor README for the `dataLayer` layout.

## Disaggregated Serving

Set `endpointRoleLabel` to the label distinguishing prefill from decode pods. TPOT is
automatically neutralized for prefill endpoints (`TPOTValid=true`, `TPOTHeadroom=0`),
ensuring TPOT doesn't affect scoring, admission, or tier classification for prefill pods.

## Files

| File | Purpose |
|------|---------|
| `plugin.go` | Struct, factory, config, per-request context, queue helpers |
| `requestcontrol_hooks.go` | PreRequest, ResponseHeader, ResponseBody hooks |
| `dataproducer_hooks.go` | Produce, Produces, Consumes |
| `training.go` | buildTrainingEntry, buildPredictionRequest, bulkPredict |
| `prediction.go` | generatePredictions, validatePrediction, TPOT neutralization |
| `running_request_tpot_slo_queue.go` | Per-pod request priority queue |
