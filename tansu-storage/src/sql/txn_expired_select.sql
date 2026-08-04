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

-- Non-terminal transactions whose timeout has elapsed, so they can be aborted.
-- prepare txn_expired_select (text) as

select
    txn.name as transaction,
    p.id as producer_id,
    pe.epoch as producer_epoch

from
    cluster c
    join txn on txn.cluster = c.id
    join producer p on p.id = txn.producer
    join txn_detail txn_d on txn_d."transaction" = txn.id
    join producer_epoch pe on pe.id = txn_d.producer_epoch

where
    c.name = $1
    and txn_d.status in ('BEGIN', 'PREPARE_COMMIT', 'PREPARE_ABORT')
    and txn_d.started_at is not null
    and txn_d.started_at + (txn_d.transaction_timeout_ms * interval '1 millisecond') < current_timestamp;
