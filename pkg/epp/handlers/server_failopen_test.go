/*
Copyright 2026.

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

package handlers

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	errcommon "github.com/llm-d/llm-d-router/pkg/common/error"
)

func TestNewStreamingServer_RoutingFailureModeFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want bool
	}{
		{name: "fail-open", env: "fail-open", want: true},
		{name: "fail-closed", env: "fail-closed", want: false},
		{name: "unset defaults to fail-closed", env: "", want: false},
		{name: "boolean-looking value is not a mode", env: "true", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(EnvRoutingFailureMode, tt.env)
			s := NewStreamingServer(nil, nil, nil, 0)
			assert.Equal(t, tt.want, s.routingFailOpen)
		})
	}
}

func TestIsRoutingFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "no candidate pods is a routing failure",
			err:  errcommon.Error{Code: errcommon.ServiceUnavailable, Msg: "no endpoints"},
			want: true,
		},
		{
			name: "flow-control rejection is a routing failure",
			err:  errcommon.Error{Code: errcommon.ResourceExhausted, Msg: "queue full"},
			want: true,
		},
		{
			name: "bad request is a client error, not a routing failure",
			err:  errcommon.Error{Code: errcommon.BadRequest, Msg: "malformed body"},
			want: false,
		},
		{
			name: "internal error is not a routing failure",
			err:  errcommon.Error{Code: errcommon.Internal, Msg: "boom"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isRoutingFailure(tt.err))
		})
	}
}

// TestSendFailOpenContinue_WithBody: the continue exchange is a header response with no
// endpoint pick (no header mutation, no dynamic metadata) followed by the duplex body echo
// ending with EndOfStream — the shape the proxy needs to resume the request untouched.
func TestSendFailOpenContinue_WithBody(t *testing.T) {
	t.Parallel()
	srv := &mockProcessServer{}
	body := bytes.Repeat([]byte("x"), 70000) // > one 62000-byte chunk, forces multi-chunk echo

	require.NoError(t, sendFailOpenContinue(srv, body))
	require.GreaterOrEqual(t, len(srv.sentResponses), 3, "header response + at least two body chunks")

	headerResp := srv.sentResponses[0].GetRequestHeaders()
	require.NotNil(t, headerResp, "first response must be a header response")
	assert.Nil(t, headerResp.GetResponse().GetHeaderMutation(), "must not set any header (no endpoint pick)")
	assert.Nil(t, srv.sentResponses[0].DynamicMetadata, "must not attach endpoint metadata")

	echoed := make([]byte, 0, len(body))
	for i, resp := range srv.sentResponses[1:] {
		bodyResp := resp.GetRequestBody()
		require.NotNil(t, bodyResp, "response %d must be a body response", i+1)
		streamed := bodyResp.GetResponse().GetBodyMutation().GetStreamedResponse()
		require.NotNil(t, streamed, "body echo must be duplex-shaped")
		echoed = append(echoed, streamed.Body...)
		if i == len(srv.sentResponses[1:])-1 {
			assert.True(t, streamed.EndOfStream, "last body chunk must carry EndOfStream")
		} else {
			assert.False(t, streamed.EndOfStream)
		}
	}
	assert.Equal(t, body, echoed, "body must be echoed verbatim")
}

func TestSendFailOpenContinue_NoBody(t *testing.T) {
	t.Parallel()
	srv := &mockProcessServer{}

	require.NoError(t, sendFailOpenContinue(srv, nil))
	require.Len(t, srv.sentResponses, 1, "header response only when no body was received")
	require.NotNil(t, srv.sentResponses[0].GetRequestHeaders())
}
