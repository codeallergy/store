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

type GetOperation struct {
	DataStore                 // should be initialized
	Context       context.Context // should be initialized
	key       []byte
	required  bool
}

func (t *GetOperation) Required() *GetOperation {
	t.required = true
	return t
}

func (t *GetOperation) ByKey(formatKey string, args... interface{}) *GetOperation {
	if len(args) > 0 {
		t.key = []byte(fmt.Sprintf(formatKey, args...))
	} else {
		t.key = []byte(formatKey)
	}
	return t
}

func (t *GetOperation) ByRawKey(key []byte) *GetOperation {
	t.key = key
	return t
}

func (t *GetOperation) ToProto(container proto.Message) error {
	var ttl int
	var version int64
	value, err := t.GetRaw(t.Context, t.key, &ttl, &version, t.required)
	if err != nil || value == nil {
		return err
	}
	return proto.Unmarshal(value, container)
}

func (t *GetOperation) ToBinary() ([]byte, error) {
	var ttl int
	var version int64
	return t.GetRaw(t.Context, t.key, &ttl, &version, t.required)
}

func (t *GetOperation) ToString() (string, error) {
	var ttl int
	var version int64
	content, err :=  t.GetRaw(t.Context, t.key, &ttl, &version, t.required)
	if err != nil || content == nil {
		return "", err
	}
	return string(content), nil
}

func (t *GetOperation) ToCounter() (uint64, error) {
	var ttl int
	var version int64
	content, err :=  t.GetRaw(t.Context, t.key, &ttl, &version, t.required)
	if err != nil || len(content) < 8 {
		return 0, err
	}
	return binary.BigEndian.Uint64(content), nil
}

func (t *GetOperation) ToEntry() (entry RawEntry, err error) {
	entry.Key = t.key
	entry.Value, err = t.GetRaw(t.Context, t.key, &entry.Ttl, &entry.Version, t.required)
	return
}

func (t *GetOperation) ToProtoEntry(factory func() proto.Message) (entry ProtoEntry, err error) {
	entry.Key = t.key
	var value []byte
	if value, err = t.GetRaw(t.Context, t.key, &entry.Ttl, &entry.Version, t.required); err != nil {
		return
	}
	if value != nil {
		entry.Value = factory()
		err = proto.Unmarshal(value, entry.Value)
	}
	return
}

