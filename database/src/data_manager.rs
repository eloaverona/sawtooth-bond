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

pub enum TransactionType<'a> {
    InsertBond(NewBond, Vec<NewCorporateDebtRating>),
    InsertHolding(NewHolding),
    UpdateHolding(&'a str, i64, i64),
    InsertOrder(NewOrder),
    InsertOrganization(NewOrganization, Vec<NewAuthorization>),
    InsertParticipant(NewParticipant),
    InsertQuote(NewQuote),
    InsertSettlement(NewSettlement),
    InsertReceipt(NewReceipt),
}

impl DataManager {
    pub fn new(pool: &ConnectionPool) -> Result<DataManager, DatabaseError> {
        let conn = pool.get_connection()?;
        let manager = DataManager { conn };
        Ok(manager)
    }

    /// Submits all state changes received in a block and
    /// deals with forks and duplicates in a single db transaction.
    /// If any of the db insertions and updates fail, all transactions will fail.
    pub fn execute_transactions_in_block(
        &self,
        transactions: Vec<TransactionType>,
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

    fn execute_transaction(&self, transaction: TransactionType) -> Result<(), DatabaseError> {
        match transaction {
            TransactionType::InsertBond(bond, corp_dept_ratings) => {
                self.insert_bond(&bond)?;
                for rating in corp_dept_ratings {
                    self.insert_corp_dept_ratings(&rating)?;
                }
                Ok(())
            }
            TransactionType::InsertHolding(holding) => self.insert_holding(&holding),
            TransactionType::UpdateHolding(address, current_block_num, max_block_num) => {
                self.update_holding(address, current_block_num, max_block_num)
            }
            TransactionType::InsertOrder(order) => self.insert_order(&order),
            TransactionType::InsertOrganization(org, authorizations) => {
                self.insert_organization(&org)?;
                for auth in authorizations {
                    self.insert_authorization(&auth)?;
                }
                Ok(())
            }
            TransactionType::InsertParticipant(participant) => {
                self.insert_participant(&participant)
            }
            TransactionType::InsertQuote(quote) => self.insert_quote(&quote),
            TransactionType::InsertSettlement(settlement) => self.insert_settlement(&settlement),
            TransactionType::InsertReceipt(receipt) => self.insert_receipt(&receipt),
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

    fn insert_authorization(&self, authorization: &NewAuthorization) -> Result<(), DatabaseError> {
        self.update_authorization(
            &authorization.organization_id,
            &authorization.participant_public_key,
            authorization.start_block_num
        )?;

        diesel::insert_into(authorizations::table)
            .values(authorization)
            .execute(&*self.conn)?;
        Ok(())
    }

    fn update_authorization(
        &self,
        organization_id: &str,
        participant_public_key: &str,
        current_block_num: i64
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

    fn insert_bond(&self, bond: &NewBond) -> Result<(), DatabaseError> {
        diesel::insert_into(bonds::table)
            .values(bond)
            .execute(&*self.conn)?;
        Ok(())
    }

    fn insert_corp_dept_ratings(
        &self,
        corp_dept_ratings: &NewCorporateDebtRating,
    ) -> Result<(), DatabaseError> {
        diesel::insert_into(corporate_debt_ratings::table)
            .values(corp_dept_ratings)
            .execute(&*self.conn)?;
        Ok(())
    }

    fn insert_holding(&self, holding: &NewHolding) -> Result<(), DatabaseError> {
        diesel::insert_into(holdings::table)
            .values(holding)
            .execute(&*self.conn)?;
        Ok(())
    }

    fn update_holding(
        &self,
        address: &str,
        current_block_num: i64,
        max_block_num: i64,
    ) -> Result<(), DatabaseError> {
        let holdings_query = holdings::table
            .filter(holdings::address.eq(address))
            .filter(holdings::end_block_num.eq(max_block_num));

        diesel::update(holdings_query)
            .set(holdings::end_block_num.eq(current_block_num))
            .execute(&*self.conn)?;
        Ok(())
    }

    fn insert_order(&self, order: &NewOrder) -> Result<(), DatabaseError> {
        diesel::insert_into(orders::table)
            .values(order)
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

    fn insert_participant(&self, participant: &NewParticipant) -> Result<(), DatabaseError> {
        self.update_participant(
            &participant.public_key,
            participant.start_block_num,
            participant.end_block_num,
        )?;

        diesel::insert_into(participants::table)
            .values(participant)
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

    fn insert_quote(&self, quote: &NewQuote) -> Result<(), DatabaseError> {
        diesel::insert_into(quotes::table)
            .values(quote)
            .execute(&*self.conn)?;
        Ok(())
    }

    fn insert_settlement(&self, settlement: &NewSettlement) -> Result<(), DatabaseError> {
        diesel::insert_into(settlements::table)
            .values(settlement)
            .execute(&*self.conn)?;
        Ok(())
    }

    fn insert_receipt(&self, receipt: &NewReceipt) -> Result<(), DatabaseError> {
        diesel::insert_into(receipts::table)
            .values(receipt)
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
