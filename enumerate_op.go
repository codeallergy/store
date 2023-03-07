/*
 * Copyright (c) 2022-2023 Zander Schwid & Co. LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"google.golang.org/protobuf/proto"
)

type EnumerateOperation struct {
	DataStore                 // should be initialized
	Context    context.Context // should be initialized
	prefixBin []byte
	seekBin   []byte
	batchSize int
	onlyKeys  bool
	reverse   bool
}

func (t *EnumerateOperation) ByPrefix(formatPrefix string, args... interface{}) *EnumerateOperation {
	if len(args) > 0 {
		t.prefixBin = []byte(fmt.Sprintf(formatPrefix, args...))
	} else {
		t.prefixBin = []byte(formatPrefix)
	}
	return t
}

func (t *EnumerateOperation) Seek(formatSeek string, args... interface{}) *EnumerateOperation {
	if len(args) > 0 {
		t.seekBin = []byte(fmt.Sprintf(formatSeek, args...))
	} else {
		t.seekBin = []byte(formatSeek)
	}
	return t
}

func (t *EnumerateOperation) ByRawPrefix(prefix []byte) *EnumerateOperation {
	t.prefixBin = prefix
	return t
}

func (t *EnumerateOperation) WithBatchSize(batchSize int) *EnumerateOperation {
	t.batchSize = batchSize
	return t
}

func (t *EnumerateOperation) OnlyKeys() *EnumerateOperation {
	t.onlyKeys = true
	return t
}

func (t *EnumerateOperation) Reverse() *EnumerateOperation {
	t.reverse = true
	return t
}

func (t *EnumerateOperation) Do(cb func(*RawEntry) bool) error {
	if t.batchSize <= 0 {
		t.batchSize = DefaultBatchSize
	}
	if t.seekBin == nil {
		t.seekBin = t.prefixBin
	}
	return t.DataStore.EnumerateRaw(t.Context, t.prefixBin, t.seekBin, t.batchSize, t.reverse, t.onlyKeys, cb)
}

func (t *EnumerateOperation) DoProto(factory func() proto.Message, cb func(*ProtoEntry) bool) error {
	if t.batchSize <= 0 {
		t.batchSize = DefaultBatchSize
	}
	if t.seekBin == nil {
		t.seekBin = t.prefixBin
	}
	var marshalErr error
	err := t.DataStore.EnumerateRaw(t.Context, t.prefixBin, t.seekBin, t.batchSize, t.onlyKeys, t.reverse, func(raw *RawEntry) bool {
		item := factory()
		if err := proto.Unmarshal(raw.Value, item); err != nil {
			marshalErr = err
			return false
		}
		pe := ProtoEntry{
			Key: raw.Key,
			Value: item,
			Ttl: raw.Ttl,
			Version: raw.Version,
		}
		return cb(&pe)
	})
	if err == nil {
		err = marshalErr
	}
	return err
}

func (t *EnumerateOperation) DoCounters(cb func(*CounterEntry) bool) (err error) {
	if t.batchSize <= 0 {
		t.batchSize = DefaultBatchSize
	}
	if t.seekBin == nil {
		t.seekBin = t.prefixBin
	}
	return t.DataStore.EnumerateRaw(t.Context, t.prefixBin, t.seekBin, t.batchSize, t.onlyKeys, t.reverse, func(raw *RawEntry) bool {
		var counter uint64
		if len(raw.Value) >= 8 {
			counter = binary.BigEndian.Uint64(raw.Value)
		}
		ce := CounterEntry{
			Key: raw.Key,
			Value: counter,
			Ttl: raw.Ttl,
			Version: raw.Version,
		}
		return cb(&ce)
	})
}
