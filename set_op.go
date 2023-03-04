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

type SetOperation struct {
	DataStore                  // should be initialized
	Context        context.Context // should be initialized
	key        []byte
	ttlSeconds int
}

func (t *SetOperation) ByKey(formatKey string, args... interface{}) *SetOperation {
	if len(args) > 0 {
		t.key = []byte(fmt.Sprintf(formatKey, args...))
	} else {
		t.key = []byte(formatKey)
	}
	return t
}

func (t *SetOperation) ByRawKey(key []byte) *SetOperation {
	t.key = key
	return t
}

func (t *SetOperation) WithTtl(ttlSeconds int) *SetOperation {
	t.ttlSeconds = ttlSeconds
	return t
}

func (t *SetOperation) Binary(value []byte) error {
	return t.DataStore.SetRaw(t.Context, t.key, value, t.ttlSeconds)
}

func (t *SetOperation) String(value string) error {
	return t.DataStore.SetRaw(t.Context, t.key, []byte(value), t.ttlSeconds)
}

func (t *SetOperation) Counter(value uint64) error {
	slice := make([]byte, 8)
	binary.BigEndian.PutUint64(slice, value)
	return t.DataStore.SetRaw(t.Context, t.key, slice, t.ttlSeconds)
}

func (t *SetOperation) Proto(msg proto.Message) error {
	bin, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return t.DataStore.SetRaw(t.Context, t.key, bin, t.ttlSeconds)
}
