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

package latencywindow

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/funcr"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	attrwindow "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/latencywindow"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
)

// The tests window the TPOT histogram through the extractor's TPOT window
// unless they exercise both.
const testMetricName = DefaultTPOTMetricName

// histogramFamily builds a cumulative histogram family from
// (upperBound → cumulativeCount) buckets plus sample sum/count. A +Inf bucket
// equal to count is appended automatically.
func histogramFamily(name string, buckets map[float64]uint64, sum float64, count uint64) *dto.MetricFamily {
	return &dto.MetricFamily{
		Name:   proto.String(name),
		Type:   dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{Histogram: histogram(buckets, sum, count)}},
	}
}

func histogram(buckets map[float64]uint64, sum float64, count uint64) *dto.Histogram {
	h := &dto.Histogram{
		SampleSum:   proto.Float64(sum),
		SampleCount: proto.Uint64(count),
	}
	for upperBound, cumulative := range buckets {
		h.Bucket = append(h.Bucket, &dto.Bucket{
			UpperBound:      proto.Float64(upperBound),
			CumulativeCount: proto.Uint64(cumulative),
		})
	}
	h.Bucket = append(h.Bucket, &dto.Bucket{
		UpperBound:      proto.Float64(math.Inf(+1)),
		CumulativeCount: proto.Uint64(count),
	})
	return h
}

func newTestExtractor() *Extractor {
	return NewExtractor("test", DefaultTTFTMetricName, DefaultTPOTMetricName, defaultQuantile, defaultMinWindowSamples)
}

func newTestEndpoint() fwkdl.Endpoint {
	return fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{}, fwkdl.NewMetrics())
}

func extract(t *testing.T, e *Extractor, ep fwkdl.Endpoint, family *dto.MetricFamily) {
	t.Helper()
	payload := sourcemetrics.PrometheusMetricMap{}
	if family != nil {
		payload[family.GetName()] = family
	}
	if err := e.Extract(context.Background(), fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]{
		Payload:  payload,
		Endpoint: ep,
	}); err != nil {
		t.Fatalf("Extract returned error: %v", err)
	}
}

func stored(t *testing.T, e *Extractor, ep fwkdl.Endpoint) *attrwindow.WindowedLatency {
	t.Helper()
	raw, ok := ep.GetAttributes().Get(e.TPOTDataKey().String())
	if !ok {
		return nil
	}
	info, ok := raw.(*attrwindow.WindowedLatency)
	if !ok {
		t.Fatalf("attribute has unexpected type %T", raw)
	}
	return info
}

func almostEqual(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6 {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func TestDataKeysCarryInstanceName(t *testing.T) {
	e := NewExtractor("latency-windows", DefaultTTFTMetricName, DefaultTPOTMetricName, defaultQuantile, defaultMinWindowSamples)
	wantTTFT := attrwindow.TTFTWindowDataKey.WithNonEmptyProducerName("latency-windows")
	wantTPOT := attrwindow.TPOTWindowDataKey.WithNonEmptyProducerName("latency-windows")
	if e.TTFTDataKey() != wantTTFT || e.TPOTDataKey() != wantTPOT {
		t.Errorf("keys = %v / %v, want %v / %v", e.TTFTDataKey(), e.TPOTDataKey(), wantTTFT, wantTPOT)
	}
	produced := e.Produces()
	if _, ok := produced[wantTTFT]; !ok {
		t.Errorf("Produces() must declare %v, got %v", wantTTFT, produced)
	}
	if _, ok := produced[wantTPOT]; !ok {
		t.Errorf("Produces() must declare %v, got %v", wantTPOT, produced)
	}
}

func TestExtractFirstScrapeSeedsWithoutPublishing(t *testing.T) {
	e := newTestExtractor()
	ep := newTestEndpoint()

	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.05: 100, 0.1: 200}, 15.0, 200))

	info := stored(t, e, ep)
	if info == nil {
		t.Fatal("expected seeded state attribute")
	}
	if info.WindowSamples != 0 || !info.UpdatedAt.IsZero() {
		t.Errorf("first scrape must not publish a window: %+v", info)
	}
	if info.PrevCount != 200 || info.PrevSum != 15.0 {
		t.Errorf("seeded state wrong: PrevCount=%d PrevSum=%v", info.PrevCount, info.PrevSum)
	}
}

func TestExtractPublishesWindowedDistribution(t *testing.T) {
	e := newTestExtractor()
	fixedNow := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return fixedNow }
	ep := newTestEndpoint()

	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.05: 1000, 0.1: 1000, 0.5: 1000}, 30.0, 1000))
	// Window: 100 samples in (0, 0.05], 100 in (0.05, 0.1], none higher.
	// Sum delta 12.0 over 200 samples → mean 60ms.
	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.05: 1100, 0.1: 1200, 0.5: 1200}, 42.0, 1200))

	info := stored(t, e, ep)
	if info == nil || info.WindowSamples != 200 {
		t.Fatalf("expected published window of 200 samples, got %+v", info)
	}
	// p50: target 100 → 100/100 into (0, 0.05] → 0.05s.
	almostEqual(t, "QuantileMs", info.QuantileMs, 50)
	almostEqual(t, "MeanMs", info.MeanMs, 60)
	if !info.UpdatedAt.Equal(fixedNow) {
		t.Errorf("UpdatedAt = %v, want %v", info.UpdatedAt, fixedNow)
	}
}

func TestExtractQuantileClampsToHighestFiniteBound(t *testing.T) {
	e := newTestExtractor()
	ep := newTestEndpoint()

	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.05: 0, 0.1: 0}, 0, 0))
	// All 100 window samples above the highest finite bucket (only +Inf grows).
	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.05: 0, 0.1: 0}, 50.0, 100))

	info := stored(t, e, ep)
	if info == nil || info.WindowSamples != 100 {
		t.Fatalf("expected published window, got %+v", info)
	}
	almostEqual(t, "QuantileMs clamped", info.QuantileMs, 100)
}

func TestExtractAggregatesLabelSets(t *testing.T) {
	e := newTestExtractor()
	ep := newTestEndpoint()

	twoSeries := func(c1, c2 uint64, s1, s2 float64) *dto.MetricFamily {
		return &dto.MetricFamily{
			Name: proto.String(testMetricName),
			Type: dto.MetricType_HISTOGRAM.Enum(),
			Metric: []*dto.Metric{
				{Histogram: histogram(map[float64]uint64{0.1: c1}, s1, c1)},
				{Histogram: histogram(map[float64]uint64{0.1: c2}, s2, c2)},
			},
		}
	}
	extract(t, e, ep, twoSeries(0, 0, 0, 0))
	extract(t, e, ep, twoSeries(60, 40, 3.0, 2.0))

	info := stored(t, e, ep)
	if info == nil || info.WindowSamples != 100 {
		t.Fatalf("expected aggregated window of 100 samples, got %+v", info)
	}
	almostEqual(t, "MeanMs", info.MeanMs, 50)
}

func TestExtractHistogramFamilyWithoutPayloadErrors(t *testing.T) {
	e := newTestExtractor()
	ep := newTestEndpoint()
	family := &dto.MetricFamily{
		Name:   proto.String(testMetricName),
		Type:   dto.MetricType_HISTOGRAM.Enum(),
		Metric: []*dto.Metric{{}},
	}
	err := e.Extract(context.Background(), fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]{
		Payload:  sourcemetrics.PrometheusMetricMap{testMetricName: family},
		Endpoint: ep,
	})
	if err == nil {
		t.Fatal("expected an error for a histogram family without histogram payload")
	}
}

func TestExtractCounterResetReseedsWithoutPublishing(t *testing.T) {
	e := newTestExtractor()
	ep := newTestEndpoint()

	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.1: 500}, 25.0, 500))
	// Restarted engine: counters back near zero.
	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.1: 60}, 3.0, 60))

	info := stored(t, e, ep)
	if info == nil {
		t.Fatal("expected reseeded state attribute")
	}
	if info.WindowSamples != 0 || !info.UpdatedAt.IsZero() {
		t.Errorf("counter reset must not publish: %+v", info)
	}
	if info.PrevCount != 60 {
		t.Errorf("state not reseeded, PrevCount=%d", info.PrevCount)
	}
}

func TestExtractWindowBelowSampleGuardAccumulates(t *testing.T) {
	e := newTestExtractor()
	ep := newTestEndpoint()

	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.1: 0}, 0, 0))
	// Below minWindowSamples: boundary must not advance, nothing published.
	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.1: 30}, 1.5, 30))
	if info := stored(t, e, ep); info.WindowSamples != 0 || info.PrevCount != 0 {
		t.Fatalf("sparse window must accumulate: %+v", info)
	}
	// Accumulated window now 60 ≥ guard → publish spanning both scrapes.
	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.1: 60}, 3.0, 60))
	info := stored(t, e, ep)
	if info.WindowSamples != 60 {
		t.Fatalf("expected accumulated window of 60 samples, got %+v", info)
	}
	almostEqual(t, "MeanMs", info.MeanMs, 50)
}

func TestExtractConfiguredMinWindowSamples(t *testing.T) {
	e := NewExtractor("test", DefaultTTFTMetricName, DefaultTPOTMetricName, defaultQuantile, 10)
	ep := newTestEndpoint()

	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.1: 0}, 0, 0))
	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.1: 10}, 0.5, 10))
	info := stored(t, e, ep)
	if info.WindowSamples != 10 {
		t.Fatalf("expected a window of 10 samples with minWindowSamples=10, got %+v", info)
	}
}

func TestExtractMissingFamilyIsNoOp(t *testing.T) {
	e := newTestExtractor()
	ep := newTestEndpoint()

	extract(t, e, ep, nil)
	if info := stored(t, e, ep); info != nil {
		t.Fatalf("expected no attribute, got %+v", info)
	}

	// A non-histogram family under the metric name is ignored too.
	extract(t, e, ep, &dto.MetricFamily{
		Name: proto.String(testMetricName),
		Type: dto.MetricType_GAUGE.Enum(),
	})
	if info := stored(t, e, ep); info != nil {
		t.Fatalf("expected no attribute for non-histogram, got %+v", info)
	}
}

func TestFactoryDefaults(t *testing.T) {
	for _, raw := range []string{``, `{}`} {
		var decoder *json.Decoder
		if raw != "" {
			decoder = json.NewDecoder(strings.NewReader(raw))
		}
		p, err := Factory("latency-windows", decoder, nil)
		if err != nil {
			t.Fatalf("Factory(%q) error: %v", raw, err)
		}
		e, ok := p.(*Extractor)
		if !ok {
			t.Fatalf("unexpected plugin type %T", p)
		}
		if e.ttft.metricName != DefaultTTFTMetricName || e.tpot.metricName != DefaultTPOTMetricName {
			t.Errorf("metric names = %q / %q", e.ttft.metricName, e.tpot.metricName)
		}
		if e.quantile != 0.5 {
			t.Errorf("quantile = %v, want 0.5", e.quantile)
		}
		if e.minWindowSamples != defaultMinWindowSamples {
			t.Errorf("minWindowSamples = %d, want %d", e.minWindowSamples, defaultMinWindowSamples)
		}
		if e.TypedName().Type != ExtractorType || e.TypedName().Name != "latency-windows" {
			t.Errorf("TypedName() = %v", e.TypedName())
		}
	}
}

func TestFactoryOverrides(t *testing.T) {
	p, err := Factory("latency-windows", json.NewDecoder(strings.NewReader(`{"ttftMetricName":"custom:ttft","tpotMetricName":"custom:tpot","quantile":0.9,"minWindowSamples":5}`)), nil)
	if err != nil {
		t.Fatalf("Factory error: %v", err)
	}
	e := p.(*Extractor)
	if e.ttft.metricName != "custom:ttft" || e.tpot.metricName != "custom:tpot" {
		t.Errorf("metric names = %q / %q", e.ttft.metricName, e.tpot.metricName)
	}
	if e.quantile != 0.9 {
		t.Errorf("quantile = %v, want 0.9", e.quantile)
	}
	if e.minWindowSamples != 5 {
		t.Errorf("minWindowSamples = %d, want 5", e.minWindowSamples)
	}
}

func TestFactoryRejectsSameMetricForBothWindows(t *testing.T) {
	if _, err := Factory("latency-windows", json.NewDecoder(strings.NewReader(`{"ttftMetricName":"same","tpotMetricName":"same"}`)), nil); err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Errorf("Factory error = %v, want same-metric validation error", err)
	}
}

func TestFactoryQuantileOutOfRange(t *testing.T) {
	for _, raw := range []string{`{"quantile": 0}`, `{"quantile": 1}`, `{"quantile": -0.5}`, `{"quantile": 1.5}`} {
		if _, err := Factory("latency-windows", json.NewDecoder(strings.NewReader(raw)), nil); err == nil || !strings.Contains(err.Error(), "quantile") {
			t.Errorf("Factory(%s) error = %v, want quantile validation error", raw, err)
		}
	}
}

func TestFactoryMinWindowSamplesZeroRejected(t *testing.T) {
	if _, err := Factory("latency-windows", json.NewDecoder(strings.NewReader(`{"minWindowSamples":0}`)), nil); err == nil || !strings.Contains(err.Error(), "minWindowSamples") {
		t.Errorf("Factory error = %v, want minWindowSamples validation error", err)
	}
}

func TestExtractPublishesConfiguredQuantile(t *testing.T) {
	e := NewExtractor("test", DefaultTTFTMetricName, DefaultTPOTMetricName, 0.95, defaultMinWindowSamples)
	ep := newTestEndpoint()

	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.05: 1000, 0.1: 1000, 0.5: 1000}, 30.0, 1000))
	extract(t, e, ep, histogramFamily(testMetricName, map[float64]uint64{0.05: 1100, 0.1: 1200, 0.5: 1200}, 42.0, 1200))

	info := stored(t, e, ep)
	if info == nil || info.WindowSamples != 200 {
		t.Fatalf("expected published window of 200 samples, got %+v", info)
	}
	// p95: target 190 → 90/100 into (0.05, 0.1] → 0.05 + 0.9*0.05 = 0.095s.
	almostEqual(t, "QuantileMs", info.QuantileMs, 95)
}

func TestBothWindowsPublishIndependently(t *testing.T) {
	e := newTestExtractor()
	ep := newTestEndpoint()

	bothFamilies := func(ttftCount uint64, ttftSum float64, tpotCount uint64, tpotSum float64) sourcemetrics.PrometheusMetricMap {
		return sourcemetrics.PrometheusMetricMap{
			DefaultTTFTMetricName: histogramFamily(DefaultTTFTMetricName, map[float64]uint64{1: ttftCount}, ttftSum, ttftCount),
			DefaultTPOTMetricName: histogramFamily(DefaultTPOTMetricName, map[float64]uint64{0.1: tpotCount}, tpotSum, tpotCount),
		}
	}
	extractPayload := func(payload sourcemetrics.PrometheusMetricMap) {
		t.Helper()
		if err := e.Extract(context.Background(), fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]{Payload: payload, Endpoint: ep}); err != nil {
			t.Fatalf("Extract returned error: %v", err)
		}
	}

	extractPayload(bothFamilies(0, 0, 0, 0))
	// TTFT accumulates 60 requests; TPOT accumulates only 30 tokens, below the guard.
	extractPayload(bothFamilies(60, 30.0, 30, 1.5))

	ttft, ok := e.LatestTTFT(ep.GetMetadata().ID.String())
	if !ok || ttft.WindowSamples != 60 {
		t.Fatalf("ttft window = %+v, %v", ttft, ok)
	}
	almostEqual(t, "ttft MeanMs", ttft.MeanMs, 500)
	if _, ok := e.LatestTPOT(ep.GetMetadata().ID.String()); ok {
		t.Fatal("tpot window below the guard must not publish while the ttft window does")
	}
	if raw, ok := ep.GetAttributes().Get(e.TTFTDataKey().String()); !ok || raw.(*attrwindow.WindowedLatency).WindowSamples != 60 {
		t.Errorf("ttft attribute = %v, %v", raw, ok)
	}

	// The TPOT window closes on the next scrape; TTFT stays below its guard.
	extractPayload(bothFamilies(70, 35.0, 100, 5.0))
	tpot, ok := e.LatestTPOT(ep.GetMetadata().ID.String())
	if !ok || tpot.WindowSamples != 100 {
		t.Fatalf("tpot window = %+v, %v", tpot, ok)
	}
	almostEqual(t, "tpot MeanMs", tpot.MeanMs, 50)
	if ttft, _ := e.LatestTTFT(ep.GetMetadata().ID.String()); ttft.WindowSamples != 60 {
		t.Errorf("ttft window must be unchanged below its guard, got %+v", ttft)
	}
}

func TestExtractMissingFamilyWarnsOncePerEndpoint(t *testing.T) {
	e := newTestExtractor()
	epA := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Name: "pod-a"}, fwkdl.NewMetrics())
	epB := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{Name: "pod-b"}, fwkdl.NewMetrics())

	// Each window warns on its own; the assertions follow the TPOT window.
	var logged []string
	logger := funcr.New(func(prefix, args string) {
		if strings.Contains(args, testMetricName) {
			logged = append(logged, args)
		}
	}, funcr.Options{})
	ctx := logr.NewContext(context.Background(), logger)

	emptyExtract := func(ep fwkdl.Endpoint) {
		t.Helper()
		if err := e.Extract(ctx, fwkdl.PollInput[sourcemetrics.PrometheusMetricMap]{
			Payload:  sourcemetrics.PrometheusMetricMap{},
			Endpoint: ep,
		}); err != nil {
			t.Fatalf("Extract returned error: %v", err)
		}
	}

	// Repeated missing scrapes on one endpoint warn exactly once.
	emptyExtract(epA)
	emptyExtract(epA)
	emptyExtract(epA)
	if len(logged) != 1 {
		t.Fatalf("expected exactly one warning for endpoint A, got %d: %v", len(logged), logged)
	}
	if !strings.Contains(logged[0], testMetricName) || !strings.Contains(logged[0], "pod-a") {
		t.Errorf("warning must name the configured metric and the endpoint, got %q", logged[0])
	}

	// The gate is per endpoint: a second endpoint with the same problem gets
	// its own warning (the shared extractor serves the whole pool).
	emptyExtract(epB)
	if len(logged) != 2 || !strings.Contains(logged[1], "pod-b") {
		t.Fatalf("expected a second warning attributed to endpoint B, got %v", logged)
	}

	// A later successful scrape still seeds and publishes normally...
	extract(t, e, epA, histogramFamily(testMetricName, map[float64]uint64{0.1: 0}, 0, 0))
	extract(t, e, epA, histogramFamily(testMetricName, map[float64]uint64{0.1: 60}, 3.0, 60))
	info := stored(t, e, epA)
	if info == nil || info.WindowSamples != 60 {
		t.Fatalf("expected published window after recovery, got %+v", info)
	}

	// ...and a regression after recovery warns again (once per episode).
	emptyExtract(epA)
	emptyExtract(epA)
	if len(logged) != 3 || !strings.Contains(logged[2], "pod-a") {
		t.Fatalf("expected a re-warn for endpoint A after recovery, got %v", logged)
	}
}

func TestLatestTracksPublishedWindowPerEndpoint(t *testing.T) {
	e := newTestExtractor()
	fixedNow := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return fixedNow }
	epA := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: fwkdl.ID{Namespace: "ns", Name: "pod-a"}}, fwkdl.NewMetrics())
	epB := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: fwkdl.ID{Namespace: "ns", Name: "pod-b"}}, fwkdl.NewMetrics())

	if _, ok := e.LatestTPOT("ns/pod-a"); ok {
		t.Fatal("Latest must be empty before any publish")
	}
	extract(t, e, epA, histogramFamily(testMetricName, map[float64]uint64{0.1: 0}, 0, 0))
	if _, ok := e.LatestTPOT("ns/pod-a"); ok {
		t.Fatal("Latest must stay empty after a seed-only scrape")
	}
	extract(t, e, epA, histogramFamily(testMetricName, map[float64]uint64{0.1: 60}, 3.0, 60))
	extract(t, e, epB, histogramFamily(testMetricName, map[float64]uint64{0.1: 0}, 0, 0))
	extract(t, e, epB, histogramFamily(testMetricName, map[float64]uint64{0.1: 100}, 10.0, 100))

	latestA, ok := e.LatestTPOT("ns/pod-a")
	if !ok || latestA.WindowSamples != 60 || !latestA.UpdatedAt.Equal(fixedNow) {
		t.Fatalf("Latest(pod-a) = %+v, %v", latestA, ok)
	}
	almostEqual(t, "pod-a MeanMs", latestA.MeanMs, 50)
	latestB, ok := e.LatestTPOT("ns/pod-b")
	if !ok || latestB.WindowSamples != 100 {
		t.Fatalf("Latest(pod-b) = %+v, %v", latestB, ok)
	}
	almostEqual(t, "pod-b MeanMs", latestB.MeanMs, 100)

	// Latest returns a copy: mutating it must not affect the registry.
	latestA.MeanMs = 0
	if again, _ := e.LatestTPOT("ns/pod-a"); again.MeanMs != 50 {
		t.Errorf("Latest must return an independent copy, registry MeanMs = %v", again.MeanMs)
	}

	// A counter reset (endpoint restart) drops the endpoint from the registry
	// until it publishes again.
	extract(t, e, epA, histogramFamily(testMetricName, map[float64]uint64{0.1: 5}, 0.25, 5))
	if _, ok := e.LatestTPOT("ns/pod-a"); ok {
		t.Fatal("Latest must be cleared on counter reset")
	}
	extract(t, e, epA, histogramFamily(testMetricName, map[float64]uint64{0.1: 65}, 3.25, 65))
	if latest, ok := e.LatestTPOT("ns/pod-a"); !ok || latest.WindowSamples != 60 {
		t.Fatalf("Latest after republish = %+v, %v", latest, ok)
	}
}

func TestLatestPrunesEndpointsThatStoppedPublishing(t *testing.T) {
	e := newTestExtractor()
	clock := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	e.now = func() time.Time { return clock }
	gone := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: fwkdl.ID{Namespace: "ns", Name: "gone"}}, fwkdl.NewMetrics())
	alive := fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{ID: fwkdl.ID{Namespace: "ns", Name: "alive"}}, fwkdl.NewMetrics())

	extract(t, e, gone, histogramFamily(testMetricName, map[float64]uint64{0.1: 0}, 0, 0))
	extract(t, e, gone, histogramFamily(testMetricName, map[float64]uint64{0.1: 60}, 3.0, 60))
	extract(t, e, alive, histogramFamily(testMetricName, map[float64]uint64{0.1: 0}, 0, 0))
	extract(t, e, alive, histogramFamily(testMetricName, map[float64]uint64{0.1: 60}, 3.0, 60))

	// The gone endpoint never scrapes again; the alive one keeps publishing
	// past the retention horizon.
	clock = clock.Add(latestRetention + pruneInterval)
	extract(t, e, alive, histogramFamily(testMetricName, map[float64]uint64{0.1: 120}, 6.0, 120))

	if _, ok := e.LatestTPOT("ns/gone"); ok {
		t.Error("an endpoint that stopped publishing past the retention must be pruned")
	}
	if _, ok := e.LatestTPOT("ns/alive"); !ok {
		t.Error("a publishing endpoint must survive the prune")
	}
}
