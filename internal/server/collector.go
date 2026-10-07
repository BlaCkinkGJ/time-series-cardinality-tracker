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

import (
	"sort"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/cardinality"
	"github.com/BlaCkinkGJ/time-series-cardinality-tracker/internal/raft"
)

var (
	groupCardinalityDesc = prometheus.NewDesc(
		"cardinality_tracker_group_cardinality",
		"Estimated number of unique ids per group.",
		[]string{"group"},
		nil,
	)
	groupsDroppedDesc = prometheus.NewDesc(
		"cardinality_tracker_groups_dropped",
		"Groups not exported because -metrics-max-groups was exceeded.",
		nil,
		nil,
	)
	engineGroupsDesc = prometheus.NewDesc(
		"cardinality_tracker_engine_groups",
		"Number of groups held by the engine.",
		nil,
		nil,
	)
	raftTermDesc = prometheus.NewDesc(
		"cardinality_tracker_raft_term",
		"Current Raft term.",
		nil,
		nil,
	)
	raftIsLeaderDesc = prometheus.NewDesc(
		"cardinality_tracker_raft_is_leader",
		"1 when this node is the Raft leader, 0 otherwise.",
		nil,
		nil,
	)
	raftAppliedIndexDesc = prometheus.NewDesc(
		"cardinality_tracker_raft_applied_index",
		"Last Raft log index applied to the FSM.",
		nil,
		nil,
	)
)

// EngineCollector exports the per-group cardinality estimates at scrape
// time. Reading state during Collect keeps the values live without a
// background refresh loop.
//
// The export is capped at maxGroups (0 = unlimited): the group label is
// unbounded by construction, so a tracker holding thousands of groups
// would otherwise inflate the metrics store. The cap keeps the groups
// with the largest estimates and reports the rest through
// cardinality_tracker_groups_dropped.
//
// ponytail: sorts every group on every scrape (O(g log g)); fine for
// thousands of groups, cache a top-N if it ever shows up in profiles.
type EngineCollector struct {
	engine    *cardinality.Engine
	maxGroups int
}

// NewEngineCollector returns a collector reading from eng. maxGroups <= 0
// exports every group.
func NewEngineCollector(eng *cardinality.Engine, maxGroups int) *EngineCollector {
	return &EngineCollector{engine: eng, maxGroups: maxGroups}
}

// Describe implements prometheus.Collector.
func (c *EngineCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- groupCardinalityDesc
	ch <- groupsDroppedDesc
	ch <- engineGroupsDesc
}

// Collect implements prometheus.Collector.
func (c *EngineCollector) Collect(ch chan<- prometheus.Metric) {
	all := c.engine.CardinalityAll()

	groups := make([]string, 0, len(all))
	for group := range all {
		groups = append(groups, group)
	}

	dropped := 0
	if c.maxGroups > 0 && len(groups) > c.maxGroups {
		sort.Slice(groups, func(i, j int) bool {
			if all[groups[i]] != all[groups[j]] {
				return all[groups[i]] > all[groups[j]]
			}
			return groups[i] < groups[j]
		})
		dropped = len(groups) - c.maxGroups
		groups = groups[:c.maxGroups]
	}

	ch <- prometheus.MustNewConstMetric(groupsDroppedDesc, prometheus.GaugeValue, float64(dropped))
	ch <- prometheus.MustNewConstMetric(engineGroupsDesc, prometheus.GaugeValue, float64(len(all)))
	for _, group := range groups {
		ch <- prometheus.MustNewConstMetric(
			groupCardinalityDesc, prometheus.GaugeValue, float64(all[group]), group,
		)
	}
}

// RaftCollector exports Raft term, leadership and applied index. The
// same signals back the /readyz handler, so a wedged event loop shows up
// in both places.
type RaftCollector struct {
	node *raft.Node
}

// NewRaftCollector returns a collector reading from n.
func NewRaftCollector(n *raft.Node) *RaftCollector {
	return &RaftCollector{node: n}
}

// Describe implements prometheus.Collector.
func (c *RaftCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- raftTermDesc
	ch <- raftIsLeaderDesc
	ch <- raftAppliedIndexDesc
}

// Collect implements prometheus.Collector.
func (c *RaftCollector) Collect(ch chan<- prometheus.Metric) {
	term, leader, applied := c.node.Status()

	leaderVal := 0.0
	if leader {
		leaderVal = 1
	}

	ch <- prometheus.MustNewConstMetric(raftTermDesc, prometheus.GaugeValue, float64(term))
	ch <- prometheus.MustNewConstMetric(raftIsLeaderDesc, prometheus.GaugeValue, leaderVal)
	ch <- prometheus.MustNewConstMetric(raftAppliedIndexDesc, prometheus.GaugeValue, float64(applied))
}
