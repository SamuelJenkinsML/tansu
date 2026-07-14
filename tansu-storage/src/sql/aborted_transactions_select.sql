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
-- the fetch offset, so read_committed consumers can skip them.
-- prepare aborted_transactions_select (text, text, integer, bigint) as

select
    p.id as producer_id,
    txn_po.offset_start as first_offset

from
    cluster c
    join topic t on t.cluster = c.id
    join topition tp on tp.topic = t.id
    join txn on txn.cluster = c.id
    join producer p on p.id = txn.producer
    join txn_detail txn_d on txn_d."transaction" = txn.id
    join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id and txn_tp.topition = tp.id
    join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id

where
    c.name = $1
    and t.name = $2
    and tp.partition = $3
    and txn_d.status = 'ABORTED'
    and txn_po.offset_end >= $4

order by
    txn_po.offset_start;
