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
	"fmt"
)

type IncrementOperation struct {
	DataStore                  // should be initialized
	Context        context.Context // should be initialized
	key        []byte
	ttlSeconds int
	Initial    int64
	Delta      int64   // should be initialized by 1
}

func (t *IncrementOperation) ByKey(formatKey string, args... interface{}) *IncrementOperation {
	if len(args) > 0 {
		t.key = []byte(fmt.Sprintf(formatKey, args...))
	} else {
		t.key = []byte(formatKey)
	}
	return t
}

func (t *IncrementOperation) ByRawKey(key []byte) *IncrementOperation {
	t.key = key
	return t
}

func (t *IncrementOperation) WithTtl(ttlSeconds int) *IncrementOperation {
	t.ttlSeconds = ttlSeconds
	return t
}

func (t *IncrementOperation) WithInitialValue(initial int64) *IncrementOperation {
	t.Initial = initial
	return t
}

func (t *IncrementOperation) WithDelta(delta int64) *IncrementOperation {
	t.Delta = delta
	return t
}

func (t *IncrementOperation) Do() (prev int64, err error) {
	return t.DataStore.IncrementRaw(t.Context, t.key, t.Initial, t.Delta, t.ttlSeconds)
}

