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
    InsertParticipants(Vec<NewParticipant>),
    InsertOrganizations(Vec<(NewOrganization, Vec<NewAuthorization>)>),
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
            OperationType::InsertParticipants(participants) => {
                self.insert_participant(&participants)
            }
            OperationType::InsertOrganizations(organizations) => {
                for (organization, authorizations) in organizations {
                    self.insert_organization(&organization)?;
                    self.insert_authorization(&authorizations)?;
                }
                Ok(())
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

    fn insert_authorization(
        &self,
        authorizations: &[NewAuthorization],
    ) -> Result<(), DatabaseError> {
        for authorization in authorizations {
            self.update_authorization(
                &authorization.organization_id,
                &authorization.participant_public_key,
                authorization.start_block_num,
            )?;
        }
        diesel::insert_into(authorizations::table)
            .values(authorizations)
            .execute(&*self.conn)?;
        Ok(())
    }

    fn update_authorization(
        &self,
        organization_id: &str,
        participant_public_key: &str,
        current_block_num: i64,
    ) -> Result<(), DatabaseError> {
        let auths_query = authorizations::table
            .filter(authorizations::organization_id.eq(organization_id))
            .filter(authorizations::participant_public_key.eq(participant_public_key))
            .filter(authorizations::end_block_num.eq(MAX_BLOCK_NUM));

        diesel::update(auths_query)
            .set(authorizations::end_block_num.eq(current_block_num))
            .execute(&*self.conn)?;
        Ok(())
    }

    fn insert_organization(&self, organization: &NewOrganization) -> Result<(), DatabaseError> {
        self.update_organization(
            &organization.organization_id,
            organization.start_block_num,
            organization.end_block_num,
        )?;

        diesel::insert_into(organizations::table)
            .values(organization)
            .execute(&*self.conn)?;

        Ok(())
    }

    fn update_organization(
        &self,
        organization_id: &str,
        current_block_num: i64,
        max_block_num: i64,
    ) -> Result<(), DatabaseError> {
        let modified_orgs_query = organizations::table
                .filter(organizations::end_block_num.eq(max_block_num)) //max_block
                .filter(organizations::organization_id.eq(organization_id)); //same company will update

        diesel::update(modified_orgs_query)
            .set(organizations::end_block_num.eq(current_block_num))
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

    fn mock_insert_participant_operation(
        organization_id: &str,
        public_key: &str,
        start_block_num: i64,
    ) -> OperationType {
        let participant = NewParticipant {
            public_key: String::from(public_key),
            organization_id: String::from(organization_id),
            username: String::from("user1"),
            start_block_num: start_block_num,
            end_block_num: MAX_BLOCK_NUM,
        };
        OperationType::InsertParticipants(vec![participant])
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
            let participant_operation1 = mock_insert_participant_operation("T", "124324253", 3);
            let participant_operation2 = mock_insert_participant_operation("T", "443241214", 10);

            let block1 = get_block(2, "block_test1");
            let block2 = get_block(10, "block_test2");

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .execute_transactions_in_block(vec![participant_operation1], &block1)
                .expect("Error executing transactions");
            manager
                .execute_transactions_in_block(vec![participant_operation2], &block2)
                .expect("Error executing transactions");

            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");
            let block_list = blocks::table
                .load::<Block>(conn)
                .expect("Error finding block");

            //before droping fork

            assert_eq!(2, participants_list.len());
            assert_eq!(2, block_list.len());

            let forked_block = get_block(10, "forked_block");

            manager
                .execute_transactions_in_block(vec![], &forked_block)
                .expect("Error executing transactions");

            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");
            let block_list = blocks::table
                .load::<Block>(conn)
                .expect("Error finding block");

            //after droping fork there should be only one participant
            assert_eq!(1, participants_list.len());
            //after droping fork there should be two blocks, not three
            assert_eq!(2, block_list.len());
        })
    }

    /// Tests that nothing is commited to the db if the block inserted is a duplicate.
    #[test]
    fn test_duplicate_block() {
        run_test(|dbaddress| {
            let participant_operation1 = mock_insert_participant_operation("T", "124324253", 3);
            let participant_operation2 = mock_insert_participant_operation("T", "443241214", 3);

            let block1 = get_block(1, "block_test1");

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .execute_transactions_in_block(vec![participant_operation1], &block1)
                .expect("Error executing transactions");

            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");

            assert_eq!(1, participants_list.len());

            manager
                .execute_transactions_in_block(vec![participant_operation2], &block1)
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
