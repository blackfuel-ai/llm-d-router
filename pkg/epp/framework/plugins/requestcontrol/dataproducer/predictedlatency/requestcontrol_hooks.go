/*
Copyright 2025 The Kubernetes Authors.

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

package predictedlatency

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"
	latencypredictor "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/predictedlatency/latencypredictorclient"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-router/pkg/common/observability/logging"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/requestcontrol"
	fwksched "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/scheduling"
)

var _ requestcontrol.PreRequest = &PredictedLatency{}
var _ requestcontrol.ResponseHeaderProcessor = &PredictedLatency{}
var _ requestcontrol.ResponseBodyProcessor = &PredictedLatency{}

// --- RequestControl Hooks ---

func (pl *PredictedLatency) PreRequest(ctx context.Context, request *fwksched.InferenceRequest, schedulingResult *fwksched.SchedulingResult) error {
	logger := log.FromContext(ctx)
	if request == nil {
		logger.V(logutil.DEBUG).Info("PredictedLatency.PreRequest: request is nil, skipping")
		return nil
	}

	if schedulingResult == nil || len(schedulingResult.ProfileResults) == 0 {
		logger.V(logutil.TRACE).Info("PredictedLatency: Skipping PreRequest because no scheduling result was provided.")
		return nil
	}

	targetMetadata := schedulingResult.ProfileResults[schedulingResult.PrimaryProfileName].TargetEndpoints[0].GetMetadata()
	if !pl.checkPredictor(logger, targetMetadata) {
		return nil
	}

	endpointName := types.NamespacedName{
		Name:      targetMetadata.ID.Name,
		Namespace: targetMetadata.ID.Namespace,
	}

	logger.V(logutil.TRACE).Info("request ID for SLO tracking", "requestID", request.Headers[reqcommon.RequestIDHeaderKey], "endpointName", endpointName)
	if request.Headers[reqcommon.RequestIDHeaderKey] == "" {
		logger.V(logutil.DEBUG).Error(errors.New("missing request ID"), "PredictedLatency.PreRequest: Request is missing request ID header")
		return nil
	}

	id := request.Headers[reqcommon.RequestIDHeaderKey]

	actual, _ := pl.runningRequestLists.LoadOrStore(endpointName, newRequestPriorityQueue())
	endpointRequestList := actual.(*requestPriorityQueue)

	predictedLatencyCtx, err := pl.getPredictedLatencyContextForRequest(request)
	if err != nil {
		id := request.Headers[reqcommon.RequestIDHeaderKey]
		logger.V(logutil.DEBUG).Info("PredictedLatency.PreRequest: Failed to get SLO context for request", "error", err, "requestID", id)
		return nil
	}

	added := endpointRequestList.Add(id, predictedLatencyCtx.avgTPOTSLO)
	if !added {
		logger.V(logutil.TRACE).Info("PredictedLatency: Item already exists in queue", "endpointName", endpointName, "requestID", id)
	}

	predictedLatencyCtx.targetMetadata = targetMetadata
	decodeEndpoint := schedulingResult.ProfileResults[schedulingResult.PrimaryProfileName].TargetEndpoints[0]
	var prefillEndpoint fwksched.Endpoint
	if prefillResult, exists := schedulingResult.ProfileResults[ExperimentalDefaultPrefillProfile]; exists && prefillResult != nil && len(prefillResult.TargetEndpoints) > 0 {
		prefillEndpoint = prefillResult.TargetEndpoints[0]
		prefillMetadata := prefillEndpoint.GetMetadata()
		predictedLatencyCtx.prefillTargetMetadata = prefillMetadata
		logger.V(logutil.DEBUG).Info("Prefill target identified for request", "requestID", id, "prefillEndpoint", prefillMetadata.ID.String())
	} else {
		logger.V(logutil.DEBUG).Info("No prefill target identified for request", "requestID", id)
	}
	predictedLatencyCtx.schedulingResult = schedulingResult
	predictedLatencyCtx.requestReceivedTimestamp = time.Now()
	refreshLastSeenMetrics(ctx, predictedLatencyCtx)

	// Reuse the in-flight load captured for the winning endpoints during Produce.
	// The InFlightLoad attribute is a live view of the producer's tracker, and the
	// producer adds this request's own tokens in its own PreRequest hook; since
	// PreRequest hooks have no defined order, re-reading it here would make the
	// training features depend on hook ordering. Produce is DAG-ordered, so the
	// value captured there is well defined and matches the prediction features.
	if snapshot, ok := predictedLatencyCtx.inFlightLoadForEndpoints[decodeEndpoint.GetMetadata().ID.String()]; ok {
		predictedLatencyCtx.prefillTokensAtDispatch = snapshot.tokens
		predictedLatencyCtx.requestsAtDispatch = snapshot.requests
	}
	if prefillEndpoint != nil {
		if snapshot, ok := predictedLatencyCtx.inFlightLoadForEndpoints[prefillEndpoint.GetMetadata().ID.String()]; ok {
			predictedLatencyCtx.prefillTokensAtDispatchOnPrefill = snapshot.tokens
			predictedLatencyCtx.requestsAtDispatchOnPrefill = snapshot.requests
		}
	}
	predictedLatencyCtx.decodeTokensAtDispatch = 0

	processPreRequestForLatencyPrediction(ctx, predictedLatencyCtx)
	return nil
}

func (pl *PredictedLatency) ResponseHeader(ctx context.Context, request *fwksched.InferenceRequest, response *requestcontrol.Response, targetMetadata *fwkdl.EndpointMetadata) {
	logger := log.FromContext(ctx)
	if request == nil {
		logger.V(logutil.DEBUG).Info("PredictedLatency.ResponseReceived: request is nil, skipping")
		return
	}
}

// ResponseBody records the completed request's training samples and metrics
// at end of stream. Whether the response streamed or arrived whole makes no
// difference: both labels come from the target endpoint's latency windows,
// which describe every request the endpoint served over the window. Chunks
// before end of stream only refresh the last-seen metrics.
func (pl *PredictedLatency) ResponseBody(ctx context.Context, request *fwksched.InferenceRequest, response *requestcontrol.Response, targetMetadata *fwkdl.EndpointMetadata) {
	logger := log.FromContext(ctx)
	if request == nil {
		logger.V(logutil.DEBUG).Info("PredictedLatency.ResponseBody: request is nil, skipping")
		return
	}
	if !pl.checkPredictor(logger, targetMetadata) {
		return
	}

	predictedLatencyCtx, err := pl.getPredictedLatencyContextForRequest(request)
	if err != nil {
		id := request.Headers[reqcommon.RequestIDHeaderKey]
		logger.V(logutil.DEBUG).Info("PredictedLatency.ResponseBody: Failed to get SLO context", "error", err, "requestID", id)
		return
	}

	refreshLastSeenMetrics(ctx, predictedLatencyCtx)
	if !response.EndOfStream {
		return
	}

	// A context without a target never went through PreRequest (the
	// director's Produce window timed out and PreRequest skipped it); it
	// carries no dispatch features, so it yields no sample and is only
	// cleaned up.
	if predictedLatencyCtx.targetMetadata != nil {
		now := time.Now()
		pl.recordTTFTAtCompletion(ctx, request, predictedLatencyCtx, now)
		pl.recordTPOTAtCompletion(ctx, request, predictedLatencyCtx, targetMetadata, now)
	}

	id := request.Headers[reqcommon.RequestIDHeaderKey]
	pl.removeRequestFromQueue(id, predictedLatencyCtx)
	pl.deletePredictedLatencyContextForRequest(request)
}

// recordTTFTAtCompletion labels the request's TTFT with the latest
// time-to-first-token window of the endpoint that prefilled it (the prefill
// endpoint in disaggregated serving, the target otherwise), then emits the
// TTFT metrics and training sample. Without a usable window the request
// contributes no TTFT sample.
func (pl *PredictedLatency) recordTTFTAtCompletion(ctx context.Context, request *fwksched.InferenceRequest, predictedLatencyCtx *predictedLatencyCtx, now time.Time) {
	logger := log.FromContext(ctx)

	prefillMetadata := predictedLatencyCtx.prefillTargetMetadata
	ttftMetadata := predictedLatencyCtx.targetMetadata
	profileName := ""
	if prefillMetadata != nil {
		ttftMetadata = prefillMetadata
		profileName = ExperimentalDefaultPrefillProfile
	}
	window, ok := usableWindow(pl.latencyWindows.LatestTTFT, ttftMetadata.ID.String(), now)
	if !ok {
		logger.V(logutil.DEBUG).Info("No usable TTFT window for endpoint, skipping TTFT sample", "endpoint", ttftMetadata.ID.String())
		return
	}
	m, err := getLatestMetricsForProfile(predictedLatencyCtx, profileName)
	if err != nil {
		logger.V(logutil.DEBUG).Info("Skipping TTFT training due to missing metrics or schedulingResult", "error", err)
		return
	}
	predictedLatencyCtx.ttft = window.QuantileMs

	logger.V(logutil.TRACE).Info("TTFT labelled from endpoint window", "actualTTFT", predictedLatencyCtx.ttft, "predictedTTFT", predictedLatencyCtx.predictedTTFT, "windowSamples", window.WindowSamples)
	recordRequestTTFT(ctx, pl.typedName.Name, pl.typedName.Type, predictedLatencyCtx.incomingModelName, request.TargetModel, predictedLatencyCtx.ttft/1000)
	recordRequestPredictedTTFT(ctx, pl.typedName.Name, pl.typedName.Type, predictedLatencyCtx.incomingModelName, request.TargetModel, predictedLatencyCtx.predictedTTFT/1000)
	if predictedLatencyCtx.ttftSLO > 0 {
		recordRequestTTFTWithSLO(ctx, pl.typedName.Name, pl.typedName.Type, predictedLatencyCtx.incomingModelName, request.TargetModel, predictedLatencyCtx.ttft, predictedLatencyCtx.ttftSLO)
	}

	prefixCacheScore := predictedLatencyCtx.prefixCacheScoresForEndpoints[ttftMetadata.ID.Name]
	encoderMatchedSize := predictedLatencyCtx.encoderMatchedSizeForEndpoints[ttftMetadata.ID.Name]
	logger.V(logutil.DEBUG).Info("Recording TTFT training data", "ttft_ms", predictedLatencyCtx.ttft, "endpoint", ttftMetadata.ID.Name, "prefixCacheScore", prefixCacheScore)
	recordTTFTTrainingData(ctx, pl.latencypredictor, pl.config.EndpointRoleLabel, predictedLatencyCtx, m, ttftMetadata, now, prefixCacheScore, encoderMatchedSize)
}

// recordTPOTAtCompletion labels the request's TPOT with the target endpoint's
// latest inter-token-latency window, then emits the TPOT metrics and
// training sample. Without a usable window the request contributes no TPOT
// sample.
func (pl *PredictedLatency) recordTPOTAtCompletion(ctx context.Context, request *fwksched.InferenceRequest, predictedLatencyCtx *predictedLatencyCtx, targetMetadata *fwkdl.EndpointMetadata, now time.Time) {
	logger := log.FromContext(ctx)

	window, ok := usableWindow(pl.latencyWindows.LatestTPOT, predictedLatencyCtx.targetMetadata.ID.String(), now)
	if !ok {
		logger.V(logutil.DEBUG).Info("No usable TPOT window for endpoint, skipping TPOT sample", "endpoint", predictedLatencyCtx.targetMetadata.ID.String())
		return
	}
	m, err := getLatestMetricsForProfile(predictedLatencyCtx, "")
	if err != nil {
		logger.V(logutil.DEBUG).Info("Skipping TPOT training due to missing metrics or schedulingResult", "error", err)
		return
	}
	predictedLatencyCtx.avgTPOT = window.QuantileMs
	predictedLatencyCtx.avgPredictedTPOT = predictedTPOTForTarget(ctx, predictedLatencyCtx)

	logger.V(logutil.TRACE).Info("TPOT labelled from endpoint window", "actualTPOT", predictedLatencyCtx.avgTPOT, "predictedTPOT", predictedLatencyCtx.avgPredictedTPOT, "windowSamples", window.WindowSamples)
	recordRequestTPOT(ctx, pl.typedName.Name, pl.typedName.Type, predictedLatencyCtx.incomingModelName, request.TargetModel, predictedLatencyCtx.avgTPOT/1000)
	recordRequestPredictedTPOT(ctx, pl.typedName.Name, pl.typedName.Type, predictedLatencyCtx.incomingModelName, request.TargetModel, predictedLatencyCtx.avgPredictedTPOT/1000)
	if predictedLatencyCtx.avgTPOTSLO > 0 {
		recordRequestTPOTWithSLO(ctx, pl.typedName.Name, pl.typedName.Type, predictedLatencyCtx.incomingModelName, request.TargetModel, predictedLatencyCtx.avgTPOT, predictedLatencyCtx.avgTPOTSLO)
	}

	entry := buildTrainingEntry(
		pl.config.EndpointRoleLabel,
		targetMetadata,
		m,
		predictedLatencyCtx.inputTokenCount,
		0,
		predictedLatencyCtx.avgTPOT,
		now,
		0,
		0,
		0,
		0,
	)
	entry.PrefillTokensInFlight = predictedLatencyCtx.prefillTokensAtDispatch
	entry.DecodeTokensInFlight = predictedLatencyCtx.decodeTokensAtDispatch
	entry.NumRequestRunning = predictedLatencyCtx.requestsAtDispatch
	if err := pl.latencypredictor.AddTrainingDataBulk([]latencypredictor.TrainingEntry{entry}); err != nil {
		logger.V(logutil.DEBUG).Error(err, "record TPOT training failed")
	}
}

func (pl *PredictedLatency) checkPredictor(logger logr.Logger, metadata *fwkdl.EndpointMetadata) bool {
	if metadata == nil {
		logger.V(logutil.TRACE).Info("PredictedLatency: Skipping hook because no target metadata was provided.")
		return false
	}
	if pl.latencypredictor == nil {
		logger.V(logutil.TRACE).Info("PredictedLatency: Skipping hook because predictor missing")
		return false
	}
	return true
}

// processPreRequestForLatencyPrediction looks up the stored prediction for the target endpoint.
func processPreRequestForLatencyPrediction(ctx context.Context, predictedLatencyCtx *predictedLatencyCtx) {
	logger := log.FromContext(ctx)
	targetName := predictedLatencyCtx.targetMetadata.ID.Name
	if m := predictedLatencyCtx.prefillTargetMetadata; m != nil {
		targetName = m.ID.Name
	}
	if storedPred, ok := predictedLatencyCtx.predictionsForScheduling[targetName]; ok {
		logger.V(logutil.DEBUG).Info("PreRequest TTFT from stored prediction", "value_ms", storedPred.TTFT, "endpoint", targetName)
		predictedLatencyCtx.predictedTTFT = storedPred.TTFT
	} else {
		logger.V(logutil.DEBUG).Info("PreRequest: no stored prediction found for target endpoint", "endpoint", targetName)
		predictedLatencyCtx.predictedTTFT = 0
	}
}

// predictedTPOTForTarget returns the TPOT predicted for the target endpoint
// at scheduling time, or 0 when no prediction was stored for it.
func predictedTPOTForTarget(ctx context.Context, predictedLatencyCtx *predictedLatencyCtx) float64 {
	logger := log.FromContext(ctx)
	targetName := predictedLatencyCtx.targetMetadata.ID.Name
	storedPred, ok := predictedLatencyCtx.predictionsForScheduling[targetName]
	if !ok {
		logger.V(logutil.DEBUG).Info("no stored TPOT prediction found for target endpoint", "endpoint", targetName)
		return 0
	}
	logger.V(logutil.DEBUG).Info("TPOT from stored prediction", "value_ms", storedPred.TPOT, "endpoint", targetName)
	return storedPred.TPOT
}
