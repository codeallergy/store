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

type CompareAndSetOperation struct {
	DataStore                  // should be initialized
	Context    context.Context // should be initialized
	key        []byte
	ttlSeconds int
	version    int64
}

func (t *CompareAndSetOperation) ByKey(formatKey string, args... interface{}) *CompareAndSetOperation {
	if len(args) > 0 {
		t.key = []byte(fmt.Sprintf(formatKey, args...))
	} else {
		t.key = []byte(formatKey)
	}
	return t
}

func (t *CompareAndSetOperation) ByRawKey(key []byte) *CompareAndSetOperation {
	t.key = key
	return t
}

func (t *CompareAndSetOperation) WithTtl(ttlSeconds int) *CompareAndSetOperation {
	t.ttlSeconds = ttlSeconds
	return t
}

func (t *CompareAndSetOperation) WithVersion(version int64) *CompareAndSetOperation {
	t.version = version
	return t
}

func (t *CompareAndSetOperation) Binary(value []byte) (bool, error) {
	return t.DataStore.CompareAndSetRaw(t.Context, t.key, value, t.ttlSeconds, t.version)
}

func (t *CompareAndSetOperation) String(value string) (bool, error) {
	return t.DataStore.CompareAndSetRaw(t.Context, t.key, []byte(value), t.ttlSeconds, t.version)
}

func (t *CompareAndSetOperation) Counter(value uint64) (bool, error) {
	slice := make([]byte, 8)
	binary.BigEndian.PutUint64(slice, value)
	return t.DataStore.CompareAndSetRaw(t.Context, t.key, slice, t.ttlSeconds, t.version)
}

func (t *CompareAndSetOperation) Proto(msg proto.Message) (bool, error) {
	bin, err := proto.Marshal(msg)
	if err != nil {
		return false, err
	}
	return t.DataStore.CompareAndSetRaw(t.Context, t.key, bin, t.ttlSeconds, t.version)
}


