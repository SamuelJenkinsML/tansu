-- -*- mode: sql; sql-product: postgres; -*-
-- Copyright ⓒ 2026 Samuel Jenkins
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

-- Copy an aborting transaction's produced offset ranges into the
-- partition-level aborted index before txn_produce_offset / txn_topition are
-- cleared: the index must outlive the txn detail (a same-epoch re-begin
-- reuses it), so read_committed consumers keep skipping the aborted records.
-- prepare txn_aborted_range_insert_from_produce (text, text, bigint, integer) as

insert into txn_aborted_range (topition, producer, offset_start, offset_end)

select txn_tp.topition, p.id, txn_po.offset_start, txn_po.offset_end

from

cluster c
join producer p on p.cluster = c.id
join producer_epoch pe on pe.producer = p.id
join txn on txn.cluster = c.id and txn.producer = p.id
join txn_detail txn_d on txn_d."transaction" = txn.id and txn_d.producer_epoch = pe.id
join txn_topition txn_tp on txn_tp.txn_detail = txn_d.id
join txn_produce_offset txn_po on txn_po.txn_topition = txn_tp.id

where

c.name = $1
and txn.name = $2
and p.id = $3
and pe.epoch = $4;
