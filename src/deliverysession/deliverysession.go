/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package deliverysession

import (
	"strconv"
	"time"
)

type DeliverySession struct {
	name string

	// for live
	startedAt  time.Time
	sentSeqIds map[string]time.Time

	// For rewind
	firstClk time.Time
}

// New Creates a new mem files map
func New(name string) *DeliverySession {
	ds := DeliverySession{name: name, startedAt: time.Now(), sentSeqIds: map[string]time.Time{}, firstClk: time.Time{}}
	return &ds
}

func (ds *DeliverySession) AddSentSeqId(mediaType string, seqId int64) {
	key := getKey(mediaType, seqId)
	ds.sentSeqIds[key] = time.Now()
}

func (ds *DeliverySession) RemoveSentSeqId(mediaType string, seqId int64) {
	key := getKey(mediaType, seqId)
	delete(ds.sentSeqIds, key)
}

func (ds *DeliverySession) GetNumElements() int {
	return len(ds.sentSeqIds)
}

func (ds *DeliverySession) IsSentSeqId(mediaType string, seqId int64) bool {
	key := getKey(mediaType, seqId)
	_, ok := ds.sentSeqIds[key]
	return ok
}

func (ds *DeliverySession) GetStartedAt() time.Time {
	return ds.startedAt
}

func (ds *DeliverySession) SetStartedAtClk(epochMs int64) {
	ds.firstClk = time.UnixMilli(epochMs)
}

func (ds *DeliverySession) GetStartedAtClk() time.Time {
	return ds.firstClk
}

func getKey(mediaType string, seqId int64) string {
	return mediaType + "-" + strconv.FormatInt(seqId, 10)
}
