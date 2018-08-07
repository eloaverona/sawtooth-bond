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
        let pool = ConnectionPool::connect(dsn)?;
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

#[cfg(test)]
mod tests {
    use super::super::{models::*, tables_schema::*};
    use super::{DataManager, OperationType, MAX_BLOCK_NUM};

    use diesel::connection::SimpleConnection;
    use diesel::prelude::*;
    use std::panic;

    fn get_data_manager(dns: &str) -> DataManager {
        DataManager::new(&dns).expect("Failed to initialize to DataManager")
    }

    fn get_participant(
        organization_id: &str,
        public_key: &str,
        start_block_num: i64,
    ) -> NewParticipant {
        NewParticipant {
            public_key: String::from(public_key),
            organization_id: String::from(organization_id),
            username: String::from("user1"),
            start_block_num: start_block_num,
            end_block_num: MAX_BLOCK_NUM,
        }
    }

    fn get_block(block_num: i64, block_id: &str) -> Block {
        Block {
            block_num: block_num,
            block_id: Some(String::from(block_id)),
        }
    }

    #[test]
    fn test_drop_fork() {
        run_test(|dbaddress| {
            let participant1 = get_participant("T", "124324253", 3);
            let participant2 = get_participant("T", "443241214", 10);

            let block1 = get_block(2, "block_test1");
            let block2 = get_block(10, "block_test2");

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_participant(&vec![participant1])
                .expect("failed to add participant");
            manager
                .insert_participant(&vec![participant2])
                .expect("failed to add participant");

            manager
                .insert_block(&block1)
                .expect("error inserting block");
            manager
                .insert_block(&block2)
                .expect("error inserting block");

            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");
            let block_list = blocks::table
                .load::<Block>(conn)
                .expect("Error finding block");

            //before droping fork

            assert_eq!(2, participants_list.len());
            assert_eq!(2, block_list.len());

            manager.drop_fork(9).expect("Error droping fork.");

            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");
            let block_list = blocks::table
                .load::<Block>(conn)
                .expect("Error finding block");

            //after droping fork
            assert_eq!(1, participants_list.len());
            assert_eq!(1, block_list.len());
        })
    }

    #[test]
    fn test_insert_participant() {
        run_test(|dbaddress| {
            let participant = get_participant("T", "124324253", 3);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_participant(&vec![participant.clone()])
                .expect("failed to add participant");

            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");

            assert_eq!(1, participants_list.len());
            assert_eq!(
                Some(participant.organization_id),
                participants_list[0].organization_id
            );
            assert_eq!(&Some(participant.username), &participants_list[0].username);
            assert_eq!(
                Some(participant.start_block_num),
                participants_list[0].start_block_num
            );
            assert_eq!(
                Some(participant.end_block_num),
                participants_list[0].end_block_num
            );
        })
    }

    #[test]
    fn test_update_participant() {
        run_test(|dbaddress| {
            let participant = get_participant("T", "124324253", 3);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_participant(&vec![participant.clone()])
                .expect("failed to add participant");

            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");

            assert_eq!(1, participants_list.len());
            assert_eq!(
                Some(participant.start_block_num),
                participants_list[0].start_block_num
            );
            assert_eq!(
                Some(participant.end_block_num),
                participants_list[0].end_block_num
            );

            let new_block_num = 5;
            let end_block_num = MAX_BLOCK_NUM;

            manager
                .update_participant(&participant.public_key, new_block_num, end_block_num)
                .expect("failed to update participant");

            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");

            assert_eq!(1, participants_list.len());
            assert_eq!(
                Some(participant.start_block_num),
                participants_list[0].start_block_num
            );
            assert_eq!(Some(new_block_num), participants_list[0].end_block_num);
        })
    }

    #[test]
    fn test_insert_block() {
        run_test(|dbaddress| {
            let block = get_block(1236, "block_test");

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager.insert_block(&block).expect("error inserting block");

            let block_list = blocks::table
                .load::<Block>(conn)
                .expect("Error finding block");

            assert_eq!(1, block_list.len());
            assert_eq!(block.block_num, block_list[0].block_num);
            assert_eq!(block.block_id, block_list[0].block_id);
        })
    }

    /// Tests that nothing is commited to the db if the block inserted is a duplicate.
    #[test]
    fn test_duplicate_block() {
        run_test(|dbaddress| {
            let participant = get_participant("T", "124324253", 1);

            let participant2 = get_participant("T", "54543254352", 1);

            let block1 = get_block(1, "block_test1");

            let t1 = OperationType::InsertParticipant(vec![participant]);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .execute_transactions_in_block(vec![t1], &block1)
                .expect("Error executing transaction.");

            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");

            assert_eq!(1, participants_list.len());

            let t2 = OperationType::InsertParticipant(vec![participant2]);

            manager
                .execute_transactions_in_block(vec![t2], &block1)
                .expect("Error executing transaction.");

            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");

            assert_eq!(1, participants_list.len()); // check that new bond was not added.
        })
    }

    fn run_test<T>(test: T) -> ()
    where
        T: FnOnce(&str) -> () + panic::UnwindSafe,
    {
        let testpath = "sawtooth:sawtooth@test-postgres:5432/sawtooth-bond-test";

        let manager = get_data_manager(testpath);
        let conn = &*manager.conn;

        let result = panic::catch_unwind(move || test(&testpath));

        conn.batch_execute(
            "DROP SCHEMA public CASCADE;
                            CREATE SCHEMA public;",
        ).expect("Error cleaning database.");
        conn.batch_execute(include_str!("../tables/load_bond_db.sql"))
            .expect("Error loading database");

        assert!(result.is_ok())
    }

}
