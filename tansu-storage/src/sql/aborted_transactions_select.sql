-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Aborted transactions whose produced records for this topic partition overlap
-- the fetch offset, so read_committed consumers can skip them. Served from
-- the partition-level txn_aborted_range index, which outlives the producing
-- txn_detail (a same-epoch re-begin reuses the detail row and resets its
-- txn_produce_offset rows).
-- prepare aborted_transactions_select (text, text, integer, bigint) as

select
    tar.producer as producer_id,
    tar.offset_start as first_offset

from
    cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join txn_aborted_range tar on tar.topition = tp.id

where
    c.name = $1
    and t.name = $2
    and tp.partition = $3
    and tar.offset_end >= $4

order by
    tar.offset_start;
