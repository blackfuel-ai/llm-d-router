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

// Package latencywindow declares the WindowedLatency attribute: a windowed
// quantile and mean of one cumulative latency histogram scraped from an
// endpoint. The vllm-latency-window-extractor publishes one for the
// endpoint's time-to-first-token histogram and one for its inter-token
// latency histogram.
package latencywindow

import (
	"time"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

const (
	// ExtractorType is the plugin type of the extractor publishing the two
	// WindowedLatency attributes. The instance name is the producer name of
	// the attributes it publishes.
	ExtractorType = "vllm-latency-window-extractor"
)

// TTFTWindowDataKey identifies the WindowedLatency attribute computed from
// the endpoint's time-to-first-token histogram. Consumers select an
// extractor instance with WithNonEmptyProducerName.
var TTFTWindowDataKey = plugin.NewDataKey("WindowedTTFT", ExtractorType)

// TPOTWindowDataKey identifies the WindowedLatency attribute computed from
// the endpoint's inter-token latency histogram, which under continuous
// batching is the time per output token of every request the endpoint
// serves. Consumers select an extractor instance with
// WithNonEmptyProducerName.
var TPOTWindowDataKey = plugin.NewDataKey("WindowedTPOT", ExtractorType)

// WindowedLatency carries the windowed distribution of one latency histogram
// for one endpoint, together with the cumulative-counter state the extractor
// deltas the next scrape against. The window spans the histogram samples
// observed between the two most recent publishes.
type WindowedLatency struct {
	// QuantileMs is the configured quantile of the window distribution, in
	// milliseconds.
	QuantileMs float64
	// MeanMs is the window mean, in milliseconds.
	MeanMs float64
	// WindowSamples is the number of histogram samples the distribution was
	// computed from; zero while the window is being seeded.
	WindowSamples uint64
	// UpdatedAt is when the distribution was last (re)computed. Consumers
	// treat old values as stale.
	UpdatedAt time.Time

	// Cumulative-counter state from the scrape that produced the current
	// window boundary, keyed by bucket upper bound (seconds).
	PrevBuckets map[float64]uint64
	PrevSum     float64
	PrevCount   uint64
}

// Clone implements fwkdl.Cloneable.
func (w *WindowedLatency) Clone() fwkdl.Cloneable {
	if w == nil {
		return nil
	}
	c := *w
	c.PrevBuckets = make(map[float64]uint64, len(w.PrevBuckets))
	for upperBound, cumulative := range w.PrevBuckets {
		c.PrevBuckets[upperBound] = cumulative
	}
	return &c
}
