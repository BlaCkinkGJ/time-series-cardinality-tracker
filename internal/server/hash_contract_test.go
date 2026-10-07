// Copyright 2026 BlaCkinkGJ
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import "testing"

// TestHashIDContract pins the string → uint64 mapping documented in
// docs/api-spec.md §1.5. It is not an implementation detail: the stored
// sketches are keyed by the value hashID produces, so breaking these
// vectors silently invalidates persisted state and every id added by a
// client before the change. A failure here means "persistence migration
// required", not "update the constant".
func TestHashIDContract(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
	}{
		{"user-123", 17358980885433774459},
		{"user-456", 1244570398996739023},
		{"a", 9607679276477937801},
		{"", 0}, // empty ids are rejected by the API before hashing
	}
	for _, c := range cases {
		if got := hashID(c.in); got != c.want {
			t.Errorf("hashID(%q) = %d (0x%016x), want %d (0x%016x)", c.in, got, got, c.want, c.want)
		}
	}
}
