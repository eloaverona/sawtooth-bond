// Copyright 2018 Bitwise IO
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

use connection_pool::{ConnectionPool, DieselConnection};
use diesel;
use diesel::prelude::*;
use errors::DatabaseError;
use models::*;
use std::i64;
use tables_schema::*;

pub const MAX_BLOCK_NUM: i64 = i64::MAX;

pub struct DataManager {
    conn: DieselConnection,
}

pub enum OperationType {
    InsertParticipant(Vec<NewParticipant>),
}

impl DataManager {
    pub fn new(dsn: &str) -> Result<DataManager, DatabaseError> {
        let pool = ConnectionPool::connect(dsn)?gi;
        let conn = pool.get_connection()?;
        let manager = DataManager { conn };
        Ok(manager)
    }

    /// Submits all state changes received in a block and
    /// deals with forks and duplicates in a single db transaction.
    /// If any of the db insertions and updates fail, all transactions will fail.
    pub fn execute_transactions_in_block(
        &self,
        transactions: Vec<OperationType>,
        block: &Block,
    ) -> Result<(), DatabaseError> {
        let conn = &*self.conn;
        conn.transaction::<_, _, _>(|| {
            let block_in_db = self.get_block_if_exists(block.block_num)?;
            if block_in_db.is_some() {
                if self.is_fork(&block_in_db.unwrap(), block) {
                    self.drop_fork(block.block_num)?;
                }
                else {
                    return Ok(()) // if block already exists in db and is not a fork, it is a duplicate, and nothing needs to be done
                }
            }
            for transaction in transactions {
                self.execute_transaction(transaction)?;
            }
            self.insert_block(block)?;
            Ok(())
        })
    }

    fn execute_transaction(&self, transaction: OperationType) -> Result<(), DatabaseError> {
        match transaction {
            OperationType::InsertParticipant(participants) => {
                self.insert_participant(&participants)
            }
        }
    }

    fn insert_block(&self, block: &Block) -> Result<(), DatabaseError> {
        diesel::insert_into(blocks::table)
            .values(block)
            .execute(&*self.conn)?;
        Ok(())
    }

    fn get_block_if_exists(&self, block_num: i64) -> Result<Option<Block>, DatabaseError> {
        let mut blocks = blocks::table.find(block_num).load::<Block>(&*self.conn)?;
        if blocks.is_empty() {
            return Ok(None);
        }
        Ok(Some(blocks.remove(0)))
    }

    fn is_fork(&self, block_in_db: &Block, block_to_be_inserted: &Block) -> bool {
        if block_in_db.block_id == block_to_be_inserted.block_id {
            return false;
        }
        true
    }


    fn insert_participant(&self, participants: &[NewParticipant]) -> Result<(), DatabaseError> {
        for participant in participants {
            self.update_participant(
                &participant.public_key,
                participant.start_block_num,
                participant.end_block_num,
            )?;
        }
        diesel::insert_into(participants::table)
            .values(participants)
            .execute(&*self.conn)?;
        Ok(())
    }

    fn update_participant(
        &self,
        public_key: &str,
        current_block_num: i64,
        max_block_num: i64,
    ) -> Result<(), DatabaseError> {
        let modified_participant_query = participants::table
            .filter(participants::end_block_num.eq(max_block_num))
            .filter(participants::public_key.eq(&public_key));

        diesel::update(modified_participant_query)
            .set(participants::end_block_num.eq(current_block_num))
            .execute(&*self.conn)?;
        Ok(())
    }

    fn drop_fork(&self, block_num: i64) -> Result<(), DatabaseError> {
        let to_drop_query = chain_record::table.filter(chain_record::start_block_num.ge(block_num));

        diesel::delete(to_drop_query).execute(&*self.conn)?;

        let to_update_query = chain_record::table.filter(chain_record::end_block_num.ge(block_num));

        diesel::update(to_update_query)
            .set(chain_record::end_block_num.eq(MAX_BLOCK_NUM))
            .execute(&*self.conn)?;

        let blocks_to_drop_query = blocks::table.filter(blocks::block_num.ge(block_num));

        diesel::delete(blocks_to_drop_query).execute(&*self.conn)?;

        Ok(())
    }
}
