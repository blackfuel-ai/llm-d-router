/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package latencywindow provides the vllm-latency-window-extractor datalayer
// plugin: it turns vLLM's cumulative time-to-first-token and inter-token
// latency histograms into windowed per-endpoint distributions (a configurable
// quantile and the mean over the samples observed since the previous publish)
// exposed as the WindowedTTFT and WindowedTPOT endpoint attributes.
package latencywindow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"sigs.k8s.io/controller-runtime/pkg/log"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	attrwindow "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/latencywindow"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
)

const (
	// ExtractorType is the plugin type name referenced from the data layer
	// configuration.
	ExtractorType = attrwindow.ExtractorType

	// DefaultTTFTMetricName is vLLM's cumulative time-to-first-token
	// histogram; its sample count increments once per request.
	DefaultTTFTMetricName = "vllm:time_to_first_token_seconds"
	// DefaultTPOTMetricName is vLLM's cumulative inter-token-latency
	// histogram; its sample count increments once per generated token.
	DefaultTPOTMetricName = "vllm:inter_token_latency_seconds"

	// defaultQuantile is the window quantile published when the config does
	// not set one.
	defaultQuantile = 0.5

	// defaultMinWindowSamples is the minimum number of histogram samples a
	// window must contain before a distribution is published. Below it the
	// window keeps accumulating across scrapes, so sparse traffic yields
	// fewer but still meaningful publishes instead of noisy per-scrape ones.
	defaultMinWindowSamples = 50

	// latestRetention bounds how long an endpoint's latest window stays in
	// the live registry after its last publish; entries older than this are
	// pruned so endpoints that left the pool do not accumulate.
	latestRetention = 10 * time.Minute
	// pruneInterval throttles the registry prune scan.
	pruneInterval = time.Minute
)

type extractorParameters struct {
	// TTFTMetricName is the cumulative time-to-first-token histogram to
	// window; defaults to vLLM's.
	TTFTMetricName string `json:"ttftMetricName"`
	// TPOTMetricName is the cumulative inter-token-latency histogram to
	// window; defaults to vLLM's.
	TPOTMetricName string `json:"tpotMetricName"`
	// Quantile is the window quantile published as QuantileMs, in (0, 1);
	// defaults to 0.5 (p50).
	Quantile *float64 `json:"quantile"`
	// MinWindowSamples is the sample count a window must reach before it is
	// published; defaults to 50.
	MinWindowSamples *uint64 `json:"minWindowSamples"`
}

var _ fwkplugin.ProducerPlugin = &Extractor{}

var _ fwkdl.PollingExtractor[sourcemetrics.PrometheusMetricMap] = &Extractor{}

// Factory instantiates a vllm-latency-window-extractor from its configuration.
func Factory(name string, rawParameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	parameters := extractorParameters{}
	if rawParameters != nil {
		if err := rawParameters.Decode(&parameters); err != nil {
			return nil, fmt.Errorf("failed to parse the parameters of the '%s' extractor - %w", ExtractorType, err)
		}
	}
	if parameters.TTFTMetricName == "" {
		parameters.TTFTMetricName = DefaultTTFTMetricName
	}
	if parameters.TPOTMetricName == "" {
		parameters.TPOTMetricName = DefaultTPOTMetricName
	}
	if parameters.TTFTMetricName == parameters.TPOTMetricName {
		return nil, fmt.Errorf("invalid configuration for '%s' extractor: ttftMetricName and tpotMetricName must differ, both are %q", ExtractorType, parameters.TTFTMetricName)
	}
	quantile := defaultQuantile
	if parameters.Quantile != nil {
		quantile = *parameters.Quantile
		if quantile <= 0 || quantile >= 1 {
			return nil, fmt.Errorf("invalid configuration for '%s' extractor: quantile must be in (0, 1), got %v", ExtractorType, quantile)
		}
	}
	minWindowSamples := uint64(defaultMinWindowSamples)
	if parameters.MinWindowSamples != nil {
		minWindowSamples = *parameters.MinWindowSamples
		if minWindowSamples == 0 {
			return nil, fmt.Errorf("invalid configuration for '%s' extractor: minWindowSamples must be > 0", ExtractorType)
		}
	}
	return NewExtractor(name, parameters.TTFTMetricName, parameters.TPOTMetricName, quantile, minWindowSamples), nil
}

// NewExtractor creates an Extractor windowing ttftMetricName into the
// WindowedTTFT attribute and tpotMetricName into the WindowedTPOT attribute,
// both under this instance's name.
func NewExtractor(name, ttftMetricName, tpotMetricName string, quantile float64, minWindowSamples uint64) *Extractor {
	return &Extractor{
		typedName:        fwkplugin.TypedName{Type: ExtractorType, Name: name},
		ttft:             newHistogramWindow(name, ttftMetricName, attrwindow.TTFTWindowDataKey),
		tpot:             newHistogramWindow(name, tpotMetricName, attrwindow.TPOTWindowDataKey),
		quantile:         quantile,
		minWindowSamples: minWindowSamples,
		now:              time.Now,
	}
}

// Extractor computes windowed TTFT and TPOT distributions per endpoint by
// deltaing the cumulative histograms between publishes. One instance serves
// every endpoint in the pool; all mutable per-endpoint state lives on the
// endpoint's AttributeMap and in each window's live registry.
type Extractor struct {
	typedName fwkplugin.TypedName
	ttft      *histogramWindow
	tpot      *histogramWindow
	// quantile is the window quantile published as QuantileMs.
	quantile         float64
	minWindowSamples uint64
	// now is the clock; overridable in tests.
	now func() time.Time
}

// TypedName returns the type and name tuple of this plugin instance.
func (e *Extractor) TypedName() fwkplugin.TypedName {
	return e.typedName
}

// TTFTDataKey returns the key of the WindowedTTFT attribute this instance
// publishes.
func (e *Extractor) TTFTDataKey() fwkplugin.DataKey {
	return e.ttft.dataKey
}

// TPOTDataKey returns the key of the WindowedTPOT attribute this instance
// publishes.
func (e *Extractor) TPOTDataKey() fwkplugin.DataKey {
	return e.tpot.dataKey
}

// Produces declares the data keys this extractor publishes.
func (e *Extractor) Produces() map[fwkplugin.DataKey]any {
	return map[fwkplugin.DataKey]any{
		e.ttft.dataKey: attrwindow.WindowedLatency{},
		e.tpot.dataKey: attrwindow.WindowedLatency{},
	}
}

// LatestTTFT returns the most recently published TTFT window for the
// endpoint with the given namespaced name (EndpointMetadata.ID.String()), or
// false when the endpoint has not published one yet or was reseeded since.
// The returned value is a copy; the caller checks UpdatedAt for staleness.
func (e *Extractor) LatestTTFT(endpointID string) (*attrwindow.WindowedLatency, bool) {
	return e.ttft.latestWindow(endpointID)
}

// LatestTPOT is LatestTTFT for the TPOT window.
func (e *Extractor) LatestTPOT(endpointID string) (*attrwindow.WindowedLatency, bool) {
	return e.tpot.latestWindow(endpointID)
}

// Extract reads both histograms from the scrape and publishes/refreshes the
// endpoint's WindowedTTFT and WindowedTPOT attributes. Each window is
// independent: a missing or non-histogram family warns once per endpoint per
// missing episode and skips that window (never fails the poll loop); a
// counter reset reseeds that window's state without publishing.
func (e *Extractor) Extract(ctx context.Context, in fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]) error {
	now := e.now()
	return errors.Join(
		e.ttft.extract(ctx, in, now, e.quantile, e.minWindowSamples),
		e.tpot.extract(ctx, in, now, e.quantile, e.minWindowSamples),
	)
}

// histogramWindow windows one cumulative histogram into one attribute.
type histogramWindow struct {
	extractorName string
	metricName    string
	dataKey       fwkplugin.DataKey
	// missingWarnedKey marks endpoints already warned about a missing metric
	// family, so the warning fires once per endpoint per missing episode
	// instead of on every scrape.
	missingWarnedKey fwkplugin.DataKey

	// latest is the live registry of the most recently published window per
	// endpoint, keyed by the endpoint's namespaced name. Endpoint attributes
	// are snapshotted for scheduling, so consumers that need the window as
	// it is at a later point (request completion) read it from here.
	latest sync.Map
	// lastPrune guards the throttled prune of latest.
	pruneMu   sync.Mutex
	lastPrune time.Time
}

func newHistogramWindow(extractorName, metricName string, dataKey fwkplugin.DataKey) *histogramWindow {
	return &histogramWindow{
		extractorName:    extractorName,
		metricName:       metricName,
		dataKey:          dataKey.WithNonEmptyProducerName(extractorName),
		missingWarnedKey: fwkplugin.NewDataKey(dataKey.String()+"MissingMetricWarned", extractorName),
	}
}

// latestEntry is one endpoint's most recently published window in the live
// registry.
type latestEntry struct {
	window      *attrwindow.WindowedLatency
	publishedAt time.Time
}

// missingWarned is the AttributeMap marker behind missingWarnedKey. Warned is
// reset on a successful scrape so a later regression warns again.
type missingWarned struct {
	Warned bool
}

// Clone implements fwkdl.Cloneable.
func (m *missingWarned) Clone() fwkdl.Cloneable {
	if m == nil {
		return nil
	}
	c := *m
	return &c
}

func (w *histogramWindow) latestWindow(endpointID string) (*attrwindow.WindowedLatency, bool) {
	raw, ok := w.latest.Load(endpointID)
	if !ok {
		return nil, false
	}
	entry := raw.(*latestEntry)
	return entry.window.Clone().(*attrwindow.WindowedLatency), true
}

func (w *histogramWindow) extract(ctx context.Context, in fwkdl.PollInput[sourcemetrics.PrometheusMetricMap], now time.Time, quantile float64, minWindowSamples uint64) error {
	attributes := in.Endpoint.GetAttributes()

	family, ok := in.Payload[w.metricName]
	if !ok || family.GetType() != dto.MetricType_HISTOGRAM {
		if !w.warnedMissing(attributes) {
			attributes.Put(w.missingWarnedKey.String(), &missingWarned{Warned: true})
			metadata := in.Endpoint.GetMetadata()
			log.FromContext(ctx).Info(
				"latency metric family missing or not a histogram; no windowed latency is published for this endpoint",
				"extractor", w.extractorName,
				"metricName", w.metricName,
				"endpoint", metadata.ID.String(),
				"pod", metadata.Name,
				"scrapedFamilies", len(in.Payload),
			)
		}
		return nil
	}
	if w.warnedMissing(attributes) {
		attributes.Put(w.missingWarnedKey.String(), &missingWarned{})
	}

	buckets, sum, count, err := aggregateHistogram(family)
	if err != nil {
		return fmt.Errorf("extractor %q on metric %q: %w", w.extractorName, w.metricName, err)
	}
	key := w.dataKey.String()
	endpointID := in.Endpoint.GetMetadata().ID.String()
	w.pruneLatest(now)

	previous, ok := fwkdl.ReadAttribute[*attrwindow.WindowedLatency](attributes, key)
	if !ok || isCounterReset(previous, buckets, count) {
		// Seed (or reseed after an endpoint restart): the previous window no
		// longer describes this endpoint, so it leaves the live registry too.
		attributes.Put(key, &attrwindow.WindowedLatency{PrevBuckets: buckets, PrevSum: sum, PrevCount: count})
		w.latest.Delete(endpointID)
		return nil
	}

	windowSamples := count - previous.PrevCount
	if windowSamples < minWindowSamples {
		// Keep the window boundary so the window accumulates across scrapes.
		return nil
	}

	deltas := bucketDeltas(previous.PrevBuckets, buckets)
	window := &attrwindow.WindowedLatency{
		QuantileMs:    windowQuantile(deltas, windowSamples, quantile) * 1000,
		MeanMs:        (sum - previous.PrevSum) / float64(windowSamples) * 1000,
		WindowSamples: windowSamples,
		UpdatedAt:     now,
		PrevBuckets:   buckets,
		PrevSum:       sum,
		PrevCount:     count,
	}
	attributes.Put(key, window)
	w.latest.Store(endpointID, &latestEntry{window: window.Clone().(*attrwindow.WindowedLatency), publishedAt: now})
	return nil
}

// warnedMissing reports whether the endpoint carries an active missing-metric
// warning marker for this window.
func (w *histogramWindow) warnedMissing(attributes fwkdl.AttributeMap) bool {
	marker, ok := fwkdl.ReadAttribute[*missingWarned](attributes, w.missingWarnedKey.String())
	return ok && marker.Warned
}

// pruneLatest drops registry entries whose last publish is older than
// latestRetention, at most once per pruneInterval.
func (w *histogramWindow) pruneLatest(now time.Time) {
	w.pruneMu.Lock()
	if now.Sub(w.lastPrune) < pruneInterval {
		w.pruneMu.Unlock()
		return
	}
	w.lastPrune = now
	w.pruneMu.Unlock()

	w.latest.Range(func(key, value any) bool {
		if now.Sub(value.(*latestEntry).publishedAt) > latestRetention {
			w.latest.Delete(key)
		}
		return true
	})
}

// aggregateHistogram sums the family's cumulative buckets, sample sum and
// sample count across all label sets. A family whose type is histogram but
// whose metrics carry no histogram payload is malformed.
func aggregateHistogram(family *dto.MetricFamily) (map[float64]uint64, float64, uint64, error) {
	buckets := map[float64]uint64{}
	var sum float64
	var count uint64
	for _, metric := range family.GetMetric() {
		histogram := metric.GetHistogram()
		if histogram == nil {
			return nil, 0, 0, errors.New("histogram family carries a metric without a histogram payload")
		}
		sum += histogram.GetSampleSum()
		count += histogram.GetSampleCount()
		for _, bucket := range histogram.GetBucket() {
			buckets[bucket.GetUpperBound()] += bucket.GetCumulativeCount()
		}
	}
	return buckets, sum, count, nil
}

// isCounterReset reports whether any cumulative counter went backwards
// (endpoint restart mid-lifecycle).
func isCounterReset(previous *attrwindow.WindowedLatency, buckets map[float64]uint64, count uint64) bool {
	if count < previous.PrevCount {
		return true
	}
	for upperBound, cumulative := range previous.PrevBuckets {
		if buckets[upperBound] < cumulative {
			return true
		}
	}
	return false
}

// bucketDelta is one bucket of the per-window distribution.
type bucketDelta struct {
	upperBound float64
	cumulative uint64 // cumulative count within the window, ascending
}

// bucketDeltas builds the window's cumulative bucket distribution, ordered by
// ascending upper bound.
func bucketDeltas(previous, current map[float64]uint64) []bucketDelta {
	deltas := make([]bucketDelta, 0, len(current))
	for upperBound, cumulative := range current {
		deltas = append(deltas, bucketDelta{upperBound: upperBound, cumulative: cumulative - previous[upperBound]})
	}
	sort.Slice(deltas, func(i, j int) bool { return deltas[i].upperBound < deltas[j].upperBound })
	return deltas
}

// windowQuantile computes the q-quantile (seconds) of the window distribution
// with histogram_quantile-style linear interpolation. Samples above the
// highest finite bucket clamp to that bound.
func windowQuantile(deltas []bucketDelta, windowSamples uint64, q float64) float64 {
	if len(deltas) == 0 || windowSamples == 0 {
		return 0
	}
	highestFinite := 0.0
	for _, delta := range deltas {
		if !math.IsInf(delta.upperBound, +1) {
			highestFinite = delta.upperBound
		}
	}

	target := q * float64(windowSamples)
	lowerBound := 0.0
	var cumulativeBelow uint64
	for _, delta := range deltas {
		if float64(delta.cumulative) >= target {
			if math.IsInf(delta.upperBound, +1) {
				return highestFinite
			}
			inBucket := delta.cumulative - cumulativeBelow
			if inBucket == 0 {
				return delta.upperBound
			}
			fraction := (target - float64(cumulativeBelow)) / float64(inBucket)
			return lowerBound + fraction*(delta.upperBound-lowerBound)
		}
		lowerBound = delta.upperBound
		cumulativeBelow = delta.cumulative
	}
	return highestFinite
}
