// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver"

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/IBM/sarama"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver/internal/metadata"
)

// Verifies that with Sarama client, when autocommit is disabled and
// periodic_commit_interval is set, offsets are committed during consumption.
func TestSarama_PeriodicCommit_CommitsDuringConsumption(t *testing.T) {
	setFranzGo(t, false) // Force Sarama implementation

	topic := "otlp_spans"
	kafkaClient, cfg := mustNewFakeCluster(t, kfake.SeedTopics(1, topic))
	cfg.ConsumerConfig.GroupID = t.Name()
	cfg.ConsumerConfig.AutoCommit.Enable = false
	cfg.PeriodicCommitInterval = 20 * time.Millisecond

	// Produce two records
	var rs []*kgo.Record
	rs = append(rs, &kgo.Record{Topic: topic, Value: []byte{1}})
	rs = append(rs, &kgo.Record{Topic: topic, Value: []byte{2}})
	require.NoError(t, kafkaClient.ProduceSync(context.Background(), rs...).FirstErr())

	// Block consumption until we observe a commit
	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	consumeFn := func(_ component.Host, _ *receiverhelper.ObsReport, _ *metadata.TelemetryBuilder) (consumeMessageFunc, error) {
		return func(ctx context.Context, _ kafkaMessage, _ attribute.Set) error {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-unblock:
				return nil
			}
		}, nil
	}

	settings, _, _ := mustNewSettings(t)
	rcv, err := newSaramaConsumer(cfg, settings, []string{topic}, consumeFn)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { assert.NoError(t, rcv.Shutdown(context.Background())) })

	// Ensure consumption started
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for consumption to start")
	}

	// Poll committed offsets until we observe progress due to periodic commits
	admin := kadm.NewClient(kafkaClient)
	assert.Eventually(t, func() bool {
		offsets, err := admin.FetchOffsets(context.Background(), cfg.ConsumerConfig.GroupID)
		if err != nil {
			return false
		}
		off, ok := offsets.Lookup(topic, 0)
		if !ok {
			return false
		}
		return off.At >= 1 // at least first record committed during consumption
	}, 10*time.Second, 20*time.Millisecond)

	// Unblock and shutdown cleanly
	close(unblock)
}

// Verifies legacy per-message commit behavior when autocommit is disabled
// and periodic commits are disabled (interval = 0s).
// fakeSaramaSession minimally implements sarama.ConsumerGroupSession for unit testing.
type fakeSaramaSession struct {
	ctx     context.Context
	commits atomic.Int64
}

func (s *fakeSaramaSession) Context() context.Context                      { return s.ctx }
func (*fakeSaramaSession) Claims() map[string][]int32                      { return nil }
func (*fakeSaramaSession) MemberID() string                                { return "" }
func (*fakeSaramaSession) GenerationID() int32                             { return 0 }
func (*fakeSaramaSession) MarkMessage(*sarama.ConsumerMessage, string)     {}
func (*fakeSaramaSession) ResetOffset(string, int32, int64, string)        {}
func (*fakeSaramaSession) MarkOffset(string, int32, int64, string)         {}
func (s *fakeSaramaSession) Commit()                                       { s.commits.Add(1) }
func (*fakeSaramaSession) Pause(map[string][]int32)                        {}
func (*fakeSaramaSession) Resume(map[string][]int32)                       {}

type fakeSaramaClaim struct {
	topic     string
	partition int32
	hwm       int64
}

func (c fakeSaramaClaim) Topic() string                        { return c.topic }
func (c fakeSaramaClaim) Partition() int32                     { return c.partition }
func (c fakeSaramaClaim) InitialOffset() int64                 { return 0 }
func (c fakeSaramaClaim) HighWaterMarkOffset() int64           { return c.hwm }
func (c fakeSaramaClaim) Messages() <-chan *sarama.ConsumerMessage { return nil }

// Verifies that with autocommit disabled and periodic interval = 0,
// handler commits per processed message (legacy behavior), deterministically.
func TestSarama_PerMessageCommit_DirectHandler_WhenPeriodicZero(t *testing.T) {
	set, _, _ := mustNewSettings(t)
	tel, err := metadata.NewTelemetryBuilder(set.TelemetrySettings)
	require.NoError(t, err)

	h := &consumerGroupHandler{
		host:                   nil,
		consumeMessage:         func(context.Context, kafkaMessage, attribute.Set) error { return nil },
		logger:                 zap.NewNop(),
		obsrecv:                nil,
		telemetryBuilder:       tel,
		autocommitEnabled:      false,
		messageMarking:         MessageMarking{After: false},
		backOff:                nil,
		periodicCommitInterval: 0,
	}

	sess := &fakeSaramaSession{ctx: context.Background()}
	claim := fakeSaramaClaim{topic: "t", partition: 0, hwm: 10}
	msg1 := &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 1, Value: []byte("a")}
	msg2 := &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 2, Value: []byte("b")}

	require.NoError(t, h.handleMessage(sess, claim, msg1))
	require.NoError(t, h.handleMessage(sess, claim, msg2))
	assert.Equal(t, int64(2), sess.commits.Load())
}
