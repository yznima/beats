// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package awss3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/elastic/beats/v7/libbeat/beat"
)

func TestEventACKTracker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	acker := newEventACKTracker(ctx)
	acker.Add(1)
	acker.ACK()

	assert.EqualValues(t, 0, acker.pendingACKs)
	assert.ErrorIs(t, acker.ctx.Err(), context.Canceled)
}

func TestEventACKTrackerNoACKs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	acker := newEventACKTracker(ctx)
	acker.Wait()

	assert.EqualValues(t, 0, acker.pendingACKs)
	assert.ErrorIs(t, acker.ctx.Err(), context.Canceled)
}

func TestEventACKHandler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Create acker. Add one pending ACK.
	acker := newEventACKTracker(ctx)
	acker.Add(1)

	// Create an ACK handler and simulate one ACKed event.
	ackHandler := newEventACKHandler()
	ackHandler.AddEvent(beat.Event{Private: acker}, true)
	ackHandler.ACKEvents(1)

	assert.EqualValues(t, 0, acker.pendingACKs)
	assert.ErrorIs(t, acker.ctx.Err(), context.Canceled)
}
