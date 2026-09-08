# vLLM Latency Window Extractor

**Type:** `vllm-latency-window-extractor`

Turns vLLM's cumulative time-to-first-token and inter-token latency
histograms, scraped by `metrics-data-source`, into windowed per-endpoint
distributions: the configured quantile and the mean over the samples the
endpoint observed since the previous publish. The two windows are
independent; each publishes when its own sample guard is reached.

## What it does

For each of the two histograms, on every scrape:

1. Looks up the metric family; it must be a histogram. A missing or
   non-histogram family is logged once per endpoint per missing episode and
   that window is skipped.
2. Sums the cumulative buckets, sample sum and sample count across every label
   set of the family.
3. On the first scrape of an endpoint, or after a counter reset (engine
   restart), seeds the window boundary without publishing.
4. Once the samples since the boundary reach `minWindowSamples`, publishes the
   window: `histogram_quantile`-style interpolated quantile (samples above the
   highest finite bucket clamp to that bound) and mean, both in milliseconds,
   and moves the boundary. Below the guard the window keeps accumulating
   across scrapes, so sparse traffic publishes less often rather than noisily.

## Attributes produced

- **Type:** `WindowedLatency` (from `attribute/latencywindow`): `QuantileMs`,
  `MeanMs`, `WindowSamples`, `UpdatedAt`.
- **Key strings:** `WindowedTTFT/<instance name>` for the time-to-first-token
  histogram and `WindowedTPOT/<instance name>` for the inter-token latency
  histogram. Under continuous batching every running request advances one
  token per decode step, so the inter-token latency window is the time per
  output token of every request the endpoint serves.

The instance also keeps a live registry of each endpoint's latest window per
histogram, read through `LatestTTFT(endpointID)` and `LatestTPOT(endpointID)`.
Endpoint attributes are snapshotted when a request is scheduled, so a
consumer that needs the window as it is when the request completes reads the
registry; the `predicted-latency-producer` labels its training samples from
it. An endpoint leaves the registry on a counter reset and after ten minutes
without a publish.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ttftMetricName` | `vllm:time_to_first_token_seconds` | Cumulative histogram behind the TTFT window |
| `tpotMetricName` | `vllm:inter_token_latency_seconds` | Cumulative histogram behind the TPOT window |
| `quantile` | `0.5` | Quantile published as `QuantileMs`, in (0, 1) |
| `minWindowSamples` | `50` | Samples a window must reach before it is published |

`minWindowSamples` sizes the windows: at a given request or token rate, a
larger guard spans more time. Size it so a window covers a typical request on
the pool when the windows label per-request training samples. The TTFT
histogram counts one sample per request and the inter-token histogram one per
token, so the same guard closes the TPOT window far more often than the TTFT
window.

## EPP config example

Declaring a `metrics-data-source` entry under `dataLayer.sources` disables the
default injection of the core metrics extractor, so `core-metrics-extractor`
stays listed explicitly.

```yaml
apiVersion: llm-d.ai/v1alpha1
kind: EndpointPickerConfig
plugins:
- type: metrics-data-source
- type: core-metrics-extractor
- type: vllm-latency-window-extractor
- type: predicted-latency-producer
  parameters:
    latencyWindowPluginRef: vllm-latency-window-extractor
dataLayer:
  sources:
  - pluginRef: metrics-data-source
    extractors:
    - pluginRef: core-metrics-extractor
    - pluginRef: vllm-latency-window-extractor
```
