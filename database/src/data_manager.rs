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

#[cfg(test)]
mod tests {
    use super::super::{
        connection_pool::ConnectionPool, custom_types::*, models::*, tables_schema::*,
    };
    use super::{DataManager, TransactionType, MAX_BLOCK_NUM};

    use diesel::connection::SimpleConnection;
    use diesel::prelude::*;
    use std::panic;

    fn get_data_manager(dns: &str) -> DataManager {
        let pool = ConnectionPool::connect(dns).expect("Failed to connect to database");
        DataManager::new(&pool).expect("Failed to initialize to DataManager")
    }

    fn get_organization(id: &str, industry: &str, start_block_num: i64) -> NewOrganization {
        NewOrganization {
            organization_id: String::from(id),
            industry: Some(String::from(industry)),
            name: Some(String::from("AT&T")),
            organization_type: OrganizationTypeEnum::TRADINGFIRM,
            start_block_num: start_block_num,
            end_block_num: MAX_BLOCK_NUM,
        }
    }

    fn get_bond(id: &str, start_block_num: i64) -> NewBond {
        NewBond {
            bond_id: String::from(id),
            issuing_organization_id: String::from("T"),
            amount_outstanding: 2250000000,
            coupon_rate: 15,
            first_coupon_date: 1333170000,
            first_settlement_date: 1325397600,
            maturity_date: 1641880800,
            face_value: 1000000000,
            coupon_type: CouponTypeEnum::FLOATING,
            coupon_frequency: CouponFrequencyEnum::QUARTERLY,
            start_block_num: start_block_num,
            end_block_num: MAX_BLOCK_NUM,
        }
    }

    fn get_rating(bond_id: &str, start_block_num: i64) -> NewCorporateDebtRating {
        NewCorporateDebtRating {
            bond_id: String::from(bond_id),
            agency: String::from("Fitch"),
            rating: String::from("BBB"),
            start_block_num: start_block_num,
            end_block_num: MAX_BLOCK_NUM,
        }
    }

    fn get_authorization(
        participant_public_key: &str,
        organization_id: &str,
        start_block_num: i64,
    ) -> NewAuthorization {
        NewAuthorization {
            participant_public_key: String::from(participant_public_key),
            organization_id: String::from(organization_id),
            role: RoleEnum::MAKERTMAKER,
            start_block_num: start_block_num,
            end_block_num: MAX_BLOCK_NUM,
        }
    }

    fn get_holding(organization_id: &str, asset_id: &str, start_block_num: i64) -> NewHolding {
        NewHolding {
            owner_organization_id: String::from(organization_id),
            asset_id: String::from(asset_id),
            asset_type: AssetTypeEnum::BOND,
            amount: 100000000,
            start_block_num: start_block_num,
            end_block_num: MAX_BLOCK_NUM,
            address: String::from("myuniqueaddressholding"),
        }
    }

    fn get_order(organization_id: &str, bond_id: &str, start_block_num: i64) -> NewOrder {
        NewOrder {
            bond_id: String::from(bond_id),
            ordering_organization_id: String::from(organization_id),
            order_id: String::from("1"),
            limit_price: None,
            limit_yield: None,
            quantity: 1000000,
            quote_id: None,
            status: OrderStatusEnum::OPEN,
            action: OrderActionEnum::SELL,
            order_type: OrderTypeEnum::MARKET,
            timestamp: 1333170000,
            start_block_num: start_block_num,
            end_block_num: MAX_BLOCK_NUM,
        }
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

    fn get_quote(
        organization_id: &str,
        bond_id: &str,
        quote_id: &str,
        start_block_num: i64,
    ) -> NewQuote {
        NewQuote {
            bond_id: String::from(bond_id),
            organization_id: String::from(organization_id),
            ask_price: 1000000000,
            ask_qty: 10000,
            bid_price: 2000000000,
            bid_qty: 10000,
            quote_id: String::from(quote_id),
            status: QuoteStatusEnum::OPEN,
            timestamp: 1333170000,
            start_block_num: start_block_num,
            end_block_num: MAX_BLOCK_NUM,
        }
    }

    fn get_settlement(order_id: &str, start_block_num: i64) -> NewSettlement {
        NewSettlement {
            order_id: String::from(order_id),
            ordering_organization_id: String::from("T"),
            quoting_organization_id: String::from("GME"),
            action: OrderActionEnum::SELL,
            bond_quantity: 10000,
            currency_amount: 100000000000,
            start_block_num: start_block_num,
            end_block_num: MAX_BLOCK_NUM,
        }
    }

    fn get_receipt(bond_id: &str, payee_organization_id: &str, start_block_num: i64) -> NewReceipt {
        NewReceipt {
            bond_id: String::from(bond_id),
            payee_organization_id: String::from(payee_organization_id),
            payment_type: PaymentTypeEnum::COUPON,
            coupon_date: 1333170000,
            amount: 100000000000,
            timestamp: 1333170000,
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
            let org = get_organization("T", "Communication", 3);
            let updated_org = get_organization("T", "Communication/Internet", 9);

            let org_auth = get_authorization("public_key1", "T", 3);
            let org_auth2 = get_authorization("public_key2", "T", 9);

            let bond = get_bond("US00206RDA77", 1);
            let bond2 = get_bond("US00999XXX99", 10);

            let rating1 = get_rating("US00206RDA77", 1);
            let rating2 = get_rating("US00999XXX99", 10);

            let holding1 = get_holding("T", "US00206RDA77", 2);
            let holding2 = get_holding("T", "US00999XXX99", 10);

            let order1 = get_order("T", "US00206RDA77", 3);
            let order2 = get_order("T", "US00999XXX99", 10);

            let participant1 = get_participant("T", "124324253", 3);
            let participant2 = get_participant("T", "443241214", 10);

            let quote1 = get_quote("T", "US00206RDA77", "quote1", 4);
            let quote2 = get_quote("T", "US00999XXX99", "quote2", 9);

            let settlement1 = get_settlement("order1", 3);
            let settlement2 = get_settlement("order2", 34);

            let receipt1 = get_receipt("US00206RDA77", "T", 3);
            let receipt2 = get_receipt("US00999XXX99", "T", 9);

            let block1 = get_block(2, "block_test1");
            let block2 = get_block(10, "block_test2");

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_organization(&org)
                .expect("error inserting organization");

            manager
                .insert_organization(&updated_org)
                .expect("error inserting organization");

            manager
                .insert_authorization(&org_auth)
                .expect("Erros inserting authorization");
            manager
                .insert_authorization(&org_auth2)
                .expect("Erros inserting authorization");

            manager.insert_bond(&bond).expect("failed to add bond");
            manager.insert_bond(&bond2).expect("failed to add bond");

            manager
                .insert_corp_dept_ratings(&rating1)
                .expect("failed to add rating");
            manager
                .insert_corp_dept_ratings(&rating2)
                .expect("failed to add rating");

            manager
                .insert_holding(&holding1)
                .expect("failed to add holding");
            manager
                .insert_holding(&holding2)
                .expect("failed to add holding");

            manager.insert_order(&order1).expect("failed to add order");
            manager.insert_order(&order2).expect("failed to add order");

            manager
                .insert_participant(&participant1)
                .expect("failed to add participant");
            manager
                .insert_participant(&participant2)
                .expect("failed to add participant");

            manager
                .insert_quote(&quote1)
                .expect("failed to insert quote.");
            manager
                .insert_quote(&quote2)
                .expect("failed to insert quote.");

            manager
                .insert_settlement(&settlement1)
                .expect("failed to insert settlement");
            manager
                .insert_settlement(&settlement2)
                .expect("failed to insert settlement");

            manager
                .insert_receipt(&receipt1)
                .expect("failed to insert receipt");
            manager
                .insert_receipt(&receipt2)
                .expect("failed to insert receipt");

            manager
                .insert_block(&block1)
                .expect("error inserting block");
            manager
                .insert_block(&block2)
                .expect("error inserting block");

            let orgs = organizations::table
                .load::<Organization>(conn)
                .expect("Error finding organization");
            let bonds = bonds::table.load::<Bond>(conn).expect("Error finding bond");
            let ratings = corporate_debt_ratings::table
                .load::<CorporateDebtRating>(conn)
                .expect("Error finding bond");
            let auths = authorizations::table
                .load::<Authorization>(conn)
                .expect("Error finding authorization");
            let holdings = holdings::table
                .load::<Holding>(conn)
                .expect("Error finding bond");
            let orders = orders::table
                .load::<Order>(conn)
                .expect("Error finding order");
            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");
            let quotes = quotes::table
                .load::<Quote>(conn)
                .expect("Error finding quote");
            let settlements_list = settlements::table
                .load::<Settlement>(conn)
                .expect("Error finding settlement");
            let receipt_list = receipts::table
                .load::<Receipt>(conn)
                .expect("Error finding receipt");
            let block_list = blocks::table
                .load::<Block>(conn)
                .expect("Error finding block");

            //before droping fork
            assert_eq!(2, orgs.len());
            assert_eq!(Some(updated_org.start_block_num), orgs[0].end_block_num);
            assert_eq!(2, auths.len());
            assert_eq!(2, bonds.len());
            assert_eq!(2, ratings.len());
            assert_eq!(2, holdings.len());
            assert_eq!(2, orders.len());
            assert_eq!(2, participants_list.len());
            assert_eq!(2, quotes.len());
            assert_eq!(2, settlements_list.len());
            assert_eq!(2, receipt_list.len());
            assert_eq!(2, block_list.len());

            manager.drop_fork(9).expect("Error droping fork.");

            let orgs = organizations::table
                .load::<Organization>(conn)
                .expect("Error finding organization");
            let bonds = bonds::table.load::<Bond>(conn).expect("Error finding bond");
            let ratings = corporate_debt_ratings::table
                .load::<CorporateDebtRating>(conn)
                .expect("Error finding bond");
            let auths = authorizations::table
                .load::<Authorization>(conn)
                .expect("Error finding authorization");
            let holdings = holdings::table
                .load::<Holding>(conn)
                .expect("Error finding bond");
            let orders = orders::table
                .load::<Order>(conn)
                .expect("Error finding order");
            let participants_list = participants::table
                .load::<Participant>(conn)
                .expect("Error finding participant");
            let quotes = quotes::table
                .load::<Quote>(conn)
                .expect("Error finding quote");
            let settlements_list = settlements::table
                .load::<Settlement>(conn)
                .expect("Error finding settlement");
            let receipt_list = receipts::table
                .load::<Receipt>(conn)
                .expect("Error finding receipt");
            let block_list = blocks::table
                .load::<Block>(conn)
                .expect("Error finding block");

            //after droping fork
            assert_eq!(Some(org.end_block_num), orgs[0].end_block_num);
            assert_eq!(1, orgs.len());
            assert_eq!(1, bonds.len());
            assert_eq!(1, ratings.len());
            assert_eq!(1, auths.len());
            assert_eq!(1, holdings.len());
            assert_eq!(1, orders.len());
            assert_eq!(1, participants_list.len());
            assert_eq!(1, quotes.len());
            assert_eq!(1, settlements_list.len());
            assert_eq!(1, receipt_list.len());
            assert_eq!(1, block_list.len());
        })
    }

    #[test]
    fn test_insert_authorization() {
        run_test(|dbaddress| {
            let a = get_authorization("public_key3", "T", 1);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager.insert_authorization(&a).expect("Insertion error");

            let auths = authorizations::table
                .load::<Authorization>(conn)
                .expect("Error finding authorization");

            assert_eq!(1, auths.len());
            assert_eq!(
                Some(a.participant_public_key),
                auths[0].participant_public_key
            );
            assert_eq!(Some(a.organization_id), auths[0].organization_id);
            assert_eq!(Some(a.role), auths[0].role);
            assert_eq!(Some(a.start_block_num), auths[0].start_block_num);
            assert_eq!(Some(a.end_block_num), auths[0].end_block_num);
        })
    }

    #[test]
    fn test_update_authorization() {
        run_test(|dbaddress| {
            let a = get_authorization("public_key1", "T", 1);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_authorization(&a)
                .expect("Error inserting authorization to db");

            let auths = authorizations::table
                .load::<Authorization>(conn)
                .expect("Error finding authorization");

            assert_eq!(1, auths.len());
            assert_eq!(Some(a.start_block_num), auths[0].start_block_num);
            assert_eq!(Some(a.end_block_num), auths[0].end_block_num);

            let new_block_num = 5;
            let end_block_num = MAX_BLOCK_NUM;

            manager
                .update_authorization(&a.organization_id, &a.participant_public_key,  new_block_num)
                .expect("Error updating authorization");

            let auths = authorizations::table
                .load::<Authorization>(conn)
                .expect("Error finding authorization");

            //check that end_block_num has been updated for outdated authorization
            assert_eq!(1, auths.len());
            assert_eq!(Some(a.start_block_num), auths[0].start_block_num);
            assert_eq!(Some(new_block_num), auths[0].end_block_num);
        })
    }

    #[test]
    fn test_insert_bond() {
        run_test(|dbaddress| {
            let bond = get_bond("US00206RDA77", 1);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager.insert_bond(&bond).expect("failed to add bond");

            let bonds = bonds::table.load::<Bond>(conn).expect("Error finding bond");

            assert_eq!(1, bonds.len());
            assert_eq!(
                Some(bond.issuing_organization_id),
                bonds[0].issuing_organization_id
            );
            assert_eq!(Some(bond.amount_outstanding), bonds[0].amount_outstanding);
            assert_eq!(Some(bond.coupon_rate), bonds[0].coupon_rate);
            assert_eq!(Some(bond.first_coupon_date), bonds[0].first_coupon_date);
            assert_eq!(
                &Some(bond.first_settlement_date),
                &bonds[0].first_settlement_date
            );
            assert_eq!(Some(bond.maturity_date), bonds[0].maturity_date);
            assert_eq!(Some(bond.face_value), bonds[0].face_value);
            assert_eq!(Some(bond.coupon_type), bonds[0].coupon_type);
            assert_eq!(Some(bond.coupon_frequency), bonds[0].coupon_frequency);
            assert_eq!(Some(bond.start_block_num), bonds[0].start_block_num);
            assert_eq!(Some(bond.end_block_num), bonds[0].end_block_num);
        })
    }

    #[test]
    fn test_insert_holding() {
        run_test(|dbaddress| {
            let holding = get_holding("T", "US00206RDA77", 2);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_holding(&holding)
                .expect("failed to add holding");

            let holdings = holdings::table
                .load::<Holding>(conn)
                .expect("Error finding bond");

            assert_eq!(1, holdings.len());
            assert_eq!(Some(holding.asset_id), holdings[0].asset_id);
            assert_eq!(
                Some(holding.owner_organization_id),
                holdings[0].owner_organization_id
            );
            assert_eq!(Some(holding.asset_type), holdings[0].asset_type);
            assert_eq!(Some(holding.amount), holdings[0].amount);
            assert_eq!(Some(holding.address), holdings[0].address);
            assert_eq!(Some(holding.start_block_num), holdings[0].start_block_num);
            assert_eq!(Some(holding.end_block_num), holdings[0].end_block_num);
        })
    }

    #[test]
    fn test_insert_corp_dept_ratings() {
        run_test(|dbaddress| {
            let rating = get_rating("US00206RDA77", 1);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_corp_dept_ratings(&rating)
                .expect("failed to add rating");

            let ratings = corporate_debt_ratings::table
                .load::<CorporateDebtRating>(conn)
                .expect("Error finding bond");

            assert_eq!(1, ratings.len());

            assert_eq!(Some(rating.bond_id), ratings[0].bond_id);
            assert_eq!(Some(rating.agency), ratings[0].agency);
            assert_eq!(Some(rating.rating), ratings[0].rating);
            assert_eq!(Some(rating.start_block_num), ratings[0].start_block_num);
            assert_eq!(Some(rating.end_block_num), ratings[0].end_block_num);
        })
    }

    #[test]
    fn test_update_holding() {
        run_test(|dbaddress| {
            let holding = get_holding("T", "US00206RDA77", 2);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_holding(&holding)
                .expect("failed to add holding");

            let holdings = holdings::table
                .load::<Holding>(conn)
                .expect("Error finding bond");

            assert_eq!(1, holdings.len());
            assert_eq!(Some(holding.start_block_num), holdings[0].start_block_num);
            assert_eq!(Some(holding.end_block_num), holdings[0].end_block_num);

            let new_block_num = 5;
            let end_block_num = MAX_BLOCK_NUM;

            manager
                .update_holding(&holding.address, new_block_num, end_block_num)
                .expect("failed to update holding");

            let holdings = holdings::table
                .load::<Holding>(conn)
                .expect("Error finding bond");

            assert_eq!(1, holdings.len());
            assert_eq!(Some(holding.start_block_num), holdings[0].start_block_num);
            assert_eq!(Some(new_block_num), holdings[0].end_block_num);
        })
    }

    #[test]
    fn test_insert_organization() {
        run_test(|dbaddress| {
            let org = get_organization("T", "Communication", 3);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_organization(&org)
                .expect("error inserting organization");

            let orgs = organizations::table
                .load::<Organization>(conn)
                .expect("Error finding organization");

            assert_eq!(1, orgs.len());
            assert_eq!(org.industry, orgs[0].industry);
            assert_eq!(Some(org.organization_id), orgs[0].organization_id);
            assert_eq!(org.name, orgs[0].name);
            assert_eq!(Some(org.organization_type), orgs[0].organization_type);
            assert_eq!(Some(org.start_block_num), orgs[0].start_block_num);
            assert_eq!(Some(org.end_block_num), orgs[0].end_block_num);
        })
    }

    #[test]
    fn test_update_organization() {
        run_test(|dbaddress| {
            let org = get_organization("T", "Communication", 3);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_organization(&org)
                .expect("error inserting organization");

            let orgs = organizations::table
                .load::<Organization>(conn)
                .expect("Error finding organization");

            assert_eq!(1, orgs.len());
            assert_eq!(Some(org.start_block_num), orgs[0].start_block_num);
            assert_eq!(Some(org.end_block_num), orgs[0].end_block_num);

            let new_block_num = 5;
            let end_block_num = MAX_BLOCK_NUM;

            manager
                .update_organization(&org.organization_id, new_block_num, end_block_num)
                .expect("failed to update organization");

            let orgs = organizations::table
                .load::<Organization>(conn)
                .expect("Error finding organization");

            assert_eq!(1, orgs.len());
            assert_eq!(Some(org.start_block_num), orgs[0].start_block_num);
            assert_eq!(Some(new_block_num), orgs[0].end_block_num);
        })
    }

    #[test]
    fn test_insert_order() {
        run_test(|dbaddress| {
            let order = get_order("T", "US00206RDA77", 3);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager.insert_order(&order).expect("failed to add order");

            let orders = orders::table
                .load::<Order>(conn)
                .expect("Error finding order");

            assert_eq!(1, orders.len());
            assert_eq!(Some(order.bond_id), orders[0].bond_id);
            assert_eq!(
                Some(order.ordering_organization_id),
                orders[0].ordering_organization_id
            );
            assert_eq!(order.limit_price, orders[0].limit_price);
            assert_eq!(order.limit_yield, orders[0].limit_yield);
            assert_eq!(Some(order.quantity), orders[0].quantity);
            assert_eq!(order.quote_id, orders[0].quote_id);
            assert_eq!(Some(order.status), orders[0].status);
            assert_eq!(Some(order.action), orders[0].action);
            assert_eq!(Some(order.order_type), orders[0].order_type);
            assert_eq!(Some(order.timestamp), orders[0].timestamp);
            assert_eq!(Some(order.start_block_num), orders[0].start_block_num);
            assert_eq!(Some(order.end_block_num), orders[0].end_block_num);
        })
    }

    #[test]
    fn test_insert_participant() {
        run_test(|dbaddress| {
            let participant = get_participant("T", "124324253", 3);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_participant(&participant)
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
                .insert_participant(&participant)
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
    fn test_insert_quote() {
        run_test(|dbaddress| {
            let quote = get_quote("T", "US00206RDA77", "quote1", 4);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_quote(&quote)
                .expect("failed to insert quote.");

            let quotes = quotes::table
                .load::<Quote>(conn)
                .expect("Error finding quote");

            assert_eq!(1, quotes.len());
            assert_eq!(Some(quote.organization_id), quotes[0].organization_id);
            assert_eq!(Some(quote.bond_id), quotes[0].bond_id);
            assert_eq!(Some(quote.ask_price), quotes[0].ask_price);
            assert_eq!(Some(quote.ask_qty), quotes[0].ask_qty);
            assert_eq!(Some(quote.bid_price), quotes[0].bid_price);
            assert_eq!(Some(quote.bid_qty), quotes[0].bid_qty);
            assert_eq!(Some(quote.status), quotes[0].status);
            assert_eq!(Some(quote.timestamp), quotes[0].timestamp);
            assert_eq!(Some(quote.start_block_num), quotes[0].start_block_num);
            assert_eq!(Some(quote.end_block_num), quotes[0].end_block_num);
        })
    }

    #[test]
    fn test_insert_settlement() {
        run_test(|dbaddress| {
            let settlement = get_settlement("order1", 34);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_settlement(&settlement)
                .expect("failed to insert settlement");

            let settlements_list = settlements::table
                .load::<Settlement>(conn)
                .expect("Error finding settlement");

            assert_eq!(1, settlements_list.len());
            assert_eq!(
                Some(settlement.ordering_organization_id),
                settlements_list[0].ordering_organization_id
            );
            assert_eq!(Some(settlement.order_id), settlements_list[0].order_id);
            assert_eq!(
                Some(settlement.quoting_organization_id),
                settlements_list[0].quoting_organization_id
            );
            assert_eq!(Some(settlement.action), settlements_list[0].action);
            assert_eq!(
                Some(settlement.bond_quantity),
                settlements_list[0].bond_quantity
            );
            assert_eq!(
                Some(settlement.currency_amount),
                settlements_list[0].currency_amount
            );
            assert_eq!(
                Some(settlement.start_block_num),
                settlements_list[0].start_block_num
            );
            assert_eq!(
                Some(settlement.end_block_num),
                settlements_list[0].end_block_num
            );
        })
    }

    #[test]
    fn test_insert_receipt() {
        run_test(|dbaddress| {
            let receipt = get_receipt("US00206RDA77", "T", 34);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .insert_receipt(&receipt)
                .expect("failed to insert receipt");

            let receipt_list = receipts::table
                .load::<Receipt>(conn)
                .expect("Error finding receipt");

            assert_eq!(1, receipt_list.len());
            assert_eq!(
                Some(receipt.payee_organization_id),
                receipt_list[0].payee_organization_id
            );
            assert_eq!(Some(receipt.bond_id), receipt_list[0].bond_id);
            assert_eq!(Some(receipt.payment_type), receipt_list[0].payment_type);
            assert_eq!(Some(receipt.coupon_date), receipt_list[0].coupon_date);
            assert_eq!(Some(receipt.amount), receipt_list[0].amount);
            assert_eq!(Some(receipt.timestamp), receipt_list[0].timestamp);
            assert_eq!(
                Some(receipt.start_block_num),
                receipt_list[0].start_block_num
            );
            assert_eq!(Some(receipt.end_block_num), receipt_list[0].end_block_num);
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

            let bond = get_bond("US00206RDA77", 2);
            let bond2 = get_bond("US00999XXX99", 10);

            let rating1 = get_rating("US00206RDA77", 2);

            let block1 = get_block(1, "block_test1");

            let t1 = TransactionType::InsertBond(bond, vec![rating1]);

            let manager = get_data_manager(dbaddress);
            let conn = &*manager.conn;

            manager
                .execute_transactions_in_block(vec![t1], &block1)
                .expect("Error executing transaction.");

            let bonds = bonds::table.load::<Bond>(conn).expect("Error finding bond");


            assert_eq!(1, bonds.len());


            let t2 = TransactionType::InsertBond(bond2, vec![]);

            let result = manager.execute_transactions_in_block(vec![t2], &block1);

            let bonds = bonds::table.load::<Bond>(conn).expect("Error finding bond");


            assert_eq!(1, bonds.len()); // check that new bond was not added.
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

        conn.batch_execute("DROP OWNED BY sawtooth")
            .expect("Error cleaning database.");
        conn.batch_execute(include_str!("../tables/load_bond_db.sql"))
            .expect("Error loading database");

        assert!(result.is_ok())
    }

}
