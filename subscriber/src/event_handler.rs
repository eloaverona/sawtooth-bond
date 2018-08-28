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

use bond_database::{
    custom_types::*, connection_pool::ConnectionPool, models::*,
    data_manager::{DataManager, TransactionType, MAX_BLOCK_NUM}
};
use sawtooth_sdk::messages::events::{EventList, Event, Event_Attribute};
use sawtooth_sdk::messages::transaction_receipt::{StateChangeList, StateChange, StateChange_Type};
use sawtooth_sdk::messages::setting::{Setting};
use protobuf;
use regex::Regex;
use bond_common::addressing::{ AddressSpace, get_address_type};
use bond_common::proto::{organization, participant, settlement, order, bond, holding, receipt, quote};


pub struct EventHandler{
    data_manager: DataManager,
}

impl EventHandler {
    pub fn new(data_manager: DataManager) -> EventHandler {
        EventHandler { data_manager }
    }

    pub fn parse_events(&self, events_data: &[u8]) -> Result<(), String> {
        let event_list: EventList = self.unpack_data(events_data)?;
        let events = event_list.get_events().to_vec();
        let block = self.parse_block(&events)?;
        let state_changes = self.parse_state_delta_events(&events)?;
        let mut transactions = Vec::<TransactionType>::new();
        for transaction in state_changes {
            transactions.push(self.parse_transaction(transaction, &block)?);
        }
        self.data_manager.execute_transactions_in_block(transactions, &block)?;

        Ok(())
    }


    fn parse_block(&self, events: &Vec<Event>) -> Result<Block, String> {
        let block_commit_event: Vec<&Event> = events.into_iter().filter(|e| e.get_event_type() == "sawtooth/block-commit").collect();
        let block_num: Vec<Event_Attribute> = block_commit_event[0].get_attributes().to_vec().into_iter().filter(|a| a.get_key() == "block_num").collect();
        let block_id: Vec<Event_Attribute> = block_commit_event[0].get_attributes().to_vec().into_iter().filter(|a| a.get_key() == "block_id").collect();

        let b = Block {
            block_num: block_num[0].get_value().parse::<i64>().map_err(|err| err.to_string())?,
            block_id: Some(block_id[0].get_value().to_string())
        };
        Ok(b)

    }

    fn parse_state_delta_events(&self, events: &Vec<Event>) -> Result<Vec<StateChange>, String> {
        let state_delta_events: Vec<&Event> = events.into_iter().filter(|e| e.get_event_type() == "sawtooth/state-delta").collect();
        let mut changes_list: Vec<StateChange> = Vec::new();
        let namespace_regex = self.get_namespace_regex();
        for event in state_delta_events {
            let mut state_change_list: StateChangeList  = self.unpack_data(event.get_data())?;
            for state_change in state_change_list.take_state_changes().to_vec() {
                if(namespace_regex.is_match(state_change.get_address())) {
                    changes_list.push(state_change);
                }
            }
        }
        Ok(changes_list)
    }

    fn unpack_data<T>(&self, data: &[u8]) -> Result<T, String>
        where
            T: protobuf::Message,
        {
            protobuf::parse_from_bytes(&data).map_err(|err| {
                err.to_string()
            })
        }

    fn get_namespace_regex(&self) -> Regex {
        Regex::new(r"^000000").unwrap()
    }

    fn parse_transaction(&self, state: StateChange, block: &Block) ->  Result<TransactionType, String> {
        let address_type = get_address_type(state.get_address());
        match address_type {
            AddressSpace::ORGANIZATION => {
                let org: organization::Organization = self.unpack_data(state.get_value())?;
                let new_org = self.get_new_organization(&org, &block);
                let new_auths = self.get_new_authorization(org.get_authorizations(), &block, org.get_organization_id());
                let transaction = TransactionType::InsertOrganization(new_org, new_auths);
                Ok(transaction)
            }
            AddressSpace::PARTICIPANT => {
                let participant: participant::Participant = self.unpack_data(state.get_value())?;
                let new_participant = self.get_new_participant(&participant, &block);
                let transaction = TransactionType::InsertParticipant(new_participant);
                Ok(transaction)

            }
            AddressSpace::SETTLEMENT => {
                let settlement: settlement::Settlement = self.unpack_data(state.get_value())?;
                let new_settlement = self.get_new_settlement(&settlement, &block);
                let transaction = TransactionType::InsertSettlement(new_settlement);
                Ok(transaction)

            }
            AddressSpace::HOLDING => {
                match state.get_field_type() {
                    StateChange_Type::DELETE => {
                        let transaction = TransactionType::UpdateHolding(state.get_address().to_string(), block.block_num, MAX_BLOCK_NUM);
                        Ok(transaction)
                    }
                    _ => {
                        let holding: holding::Holding = self.unpack_data(state.get_value())?;
                        let new_holding = self.get_new_holding(&holding, &block, state.get_address());
                        let transaction = TransactionType::InsertHolding(new_holding);
                        Ok(transaction)
                    }
                }
            }
            AddressSpace::ORDER => {
                let order: order::Order = self.unpack_data(state.get_value())?;
                let new_order = self.get_new_order(&order, &block);
                let transaction = TransactionType::InsertOrder(new_order);
                Ok(transaction)
            }
            AddressSpace::BOND => {
                let bond: bond::Bond = self.unpack_data(state.get_value())?;
                let new_bond = self.get_new_bond(&bond, &block);
                let new_ratings = self.get_new_corporate_debt_ratings(bond.get_corporate_debt_ratings(), &block, bond.get_bond_id());
                let transaction = TransactionType::InsertBond(new_bond, new_ratings);
                Ok(transaction)
            }
            AddressSpace::RECEIPT => {
                let receipt: receipt::Receipt = self.unpack_data(state.get_value())?;
                let new_receipt = self.get_new_receipt(&receipt, &block);
                let transaction = TransactionType::InsertReceipt(new_receipt);
                Ok(transaction)
            }
            AddressSpace::QUOTE => {
                let quote: quote::Quote = self.unpack_data(state.get_value())?;
                let new_quote = self.get_new_quote(&quote, &block);
                let transaction = TransactionType::InsertQuote(new_quote);
                Ok(transaction)
            }
        }
    }
    fn get_new_organization(&self, org: &organization::Organization, block: &Block) -> NewOrganization{
        NewOrganization {
            organization_id: org.get_organization_id().to_string(),
            industry: Some(org.get_industry().to_string()),
            name: Some(org.get_name().to_string()),
            organization_type: match org.get_organization_type() {
                organization::Organization_OrganizationType::TRADING_FIRM => OrganizationTypeEnum::TRADINGFIRM,
                organization::Organization_OrganizationType::PRICING_SOURCE => OrganizationTypeEnum::PRICINGSOURCE,
            },
            start_block_num: block.block_num,
            end_block_num: MAX_BLOCK_NUM
        }
    }

    fn get_new_authorization(&self, auths: &[organization::Organization_Authorization], block: &Block, organization_id: &str) -> Vec<NewAuthorization> {
        let mut authorization_list = Vec::<NewAuthorization>::new();
        for auth in auths {
            let new = NewAuthorization {
                organization_id: organization_id.to_string(),
                participant_public_key: auth.get_public_key().to_string(),
                role: match auth.get_role() {
                    organization::Organization_Authorization_Role::MARKETMAKER => RoleEnum::MAKERTMAKER,
                    organization::Organization_Authorization_Role::TRADER => RoleEnum::TRADER
                },
                start_block_num: block.block_num,
                end_block_num: MAX_BLOCK_NUM
            };
            authorization_list.push(new);
        }
        authorization_list
    }

    fn get_new_participant(&self, participant: &participant::Participant, block: &Block) -> NewParticipant {
        NewParticipant {
            public_key: participant.get_public_key().to_string(),
            organization_id: participant.get_organization_id().to_string(),
            username: participant.get_username().to_string(),
            start_block_num: block.block_num,
            end_block_num: MAX_BLOCK_NUM
        }
    }

    fn get_new_settlement(&self, settlement: &settlement::Settlement, block: &Block) -> NewSettlement {
        NewSettlement {
            order_id: settlement.get_order_id().to_string(),
            ordering_organization_id: settlement.get_ordering_organization_id().to_string(),
            quoting_organization_id: settlement.get_quoting_organization_id().to_string(),
            action: match settlement.get_action() {
                settlement::Settlement_Action::BUY => OrderActionEnum::BUY,
                settlement::Settlement_Action::SELL => OrderActionEnum::SELL
            },
            bond_quantity: settlement.get_bond_quantity() as i64,
            currency_amount: settlement.get_currency_amount() as i64,
            start_block_num: block.block_num,
            end_block_num: MAX_BLOCK_NUM
        }
    }

    fn get_new_holding(&self, holding: &holding::Holding, block: &Block, address: &str) -> NewHolding {
        NewHolding  {
            owner_organization_id: holding.get_organization_id().to_string(),
            asset_id: holding.get_asset_id().to_string(),
            asset_type: match holding.get_asset_type() {
                holding::Holding_AssetType::CURRENCY => AssetTypeEnum::CURRENCY,
                holding::Holding_AssetType::BOND => AssetTypeEnum::BOND
            },
            amount: holding.get_amount() as i64,
            start_block_num: block.block_num,
            end_block_num: MAX_BLOCK_NUM,
            address: address.to_string()
        }
    }

    fn get_new_order(&self, order: &order::Order, block: &Block) -> NewOrder {
        NewOrder {
            bond_id: order.get_order_id().to_string(),
            ordering_organization_id: order.get_ordering_organization_id().to_string(),
            order_id: order.get_order_id().to_string(),
            limit_price: Some(order.get_limit_price() as i64),
            limit_yield: Some(order.get_limit_yield() as i64),
            quantity: order.get_quantity() as i64,
            quote_id: Some(order.get_quote_id().to_string()),
            status: match order.get_status() {
                order::Order_Status::OPEN => OrderStatusEnum::OPEN,
                order::Order_Status::MATCHED => OrderStatusEnum::MATCHED,
                order::Order_Status::SETTLED => OrderStatusEnum::SETTLED,
            },
            action: match order.get_action() {
                order::Order_Action::BUY => OrderActionEnum::BUY,
                order::Order_Action::SELL => OrderActionEnum::SELL
            },
            order_type: match order.get_order_type() {
                order::Order_OrderType::MARKET => OrderTypeEnum::MARKET,
                order::Order_OrderType::LIMIT => OrderTypeEnum::LIMIT,
            },
            timestamp: order.get_timestamp() as i64,
            start_block_num: block.block_num,
            end_block_num: MAX_BLOCK_NUM,
        }
    }

    fn get_new_bond(&self, bond: &bond::Bond, block: &Block) -> NewBond {
        NewBond {
            bond_id: bond.get_bond_id().to_string(),
            issuing_organization_id: bond.get_issuing_organization_id().to_string(),
            amount_outstanding: bond.get_amount_outstanding() as i64,
            coupon_rate: bond.get_coupon_rate() as i64,
            first_coupon_date: bond.get_first_coupon_date() as i64,
            first_settlement_date: bond.get_first_settlement_date()  as i64,
            maturity_date: bond.get_maturity_date() as i64,
            face_value: bond.get_face_value() as i64,
            coupon_type: match bond.get_coupon_type() {
                bond::Bond_CouponType::FIXED => CouponTypeEnum::FIXED,
                bond::Bond_CouponType::FLOATING => CouponTypeEnum::FLOATING
            },
            coupon_frequency: match bond.get_coupon_frequency() {
                bond::Bond_CouponFrequency::QUARTERLY => CouponFrequencyEnum::QUARTERLY,
                bond::Bond_CouponFrequency::MONTHLY => CouponFrequencyEnum::MONTHLY,
                bond::Bond_CouponFrequency::DAILY => CouponFrequencyEnum::DAILY,
            },
            start_block_num: block.block_num,
            end_block_num: MAX_BLOCK_NUM
        }
    }

    fn get_new_corporate_debt_ratings(&self, ratings: &[bond::Bond_CorporateDebtRating], block: &Block, bond_id: &str) -> Vec<NewCorporateDebtRating> {
        let mut new_ratings = Vec::<NewCorporateDebtRating>::new();
        for rating in ratings {
            let new = NewCorporateDebtRating {
                bond_id: bond_id.to_string(),
                agency: rating.get_agency().to_string(),
                rating: rating.get_rating().to_string(),
                start_block_num: block.block_num,
                end_block_num: MAX_BLOCK_NUM
            };
            new_ratings.push(new);
        }
        new_ratings
    }

    fn get_new_receipt(&self, receipt: &receipt::Receipt, block: &Block) -> NewReceipt {
        NewReceipt {
            payee_organization_id: receipt.get_organization_id().to_string(),
            bond_id: receipt.get_bond_id().to_string(),
            payment_type: match receipt.get_payment_type() {
                receipt::Receipt_PaymentType::COUPON => PaymentTypeEnum::COUPON,
                receipt::Receipt_PaymentType::REDEMPTION => PaymentTypeEnum::REDEMPTION
            },
            coupon_date: receipt.get_coupon_date() as i64,
            amount: receipt.get_amount() as i64,
            timestamp: receipt.get_timestamp() as i64,
            start_block_num: block.block_num,
            end_block_num: MAX_BLOCK_NUM
        }
    }

    fn get_new_quote(&self, : &quote::Quote, block: &Block) -> NewQuote {
        NewQuote {
            bond_id: quote.get_bond_id().to_string(),
            organization_id: quote.get_organization_id().to_string(),
            ask_price: quote.get_ask_price() as i64,
            ask_qty: quote.get_ask_qty() as i64,
            bid_price: quote.get_bid_price() as i64,
            bid_qty: quote.get_bid_qty() as i64,
            payment_type: match receipt.get_payment_type() {
                quote::Quote_Status::OPEN => QuoteStatusEnum::OPEN,
                quote::Quote_Status::CLOSED => QuoteStatusEnum::CLOSED
            },
            timestamp: quote.get_timestamp() as i64,
            start_block_num: block.block_num,
            end_block_num: MAX_BLOCK_NUM
        }
    }

}

#[cfg(test)]
mod tests {
    use super::EventHandler;

    use bond_database::{
        custom_types::*, connection_pool::ConnectionPool, models::*,
        data_manager::{DataManager, TransactionType, MAX_BLOCK_NUM}
    };

    use sawtooth_sdk::messages::events::{Event, Event_Attribute};
    use sawtooth_sdk::messages::transaction_receipt::{StateChangeList, StateChange, StateChange_Type};

    fn get_event_handler() -> EventHandler {
        let dsn = "sawtooth:sawtooth@test-postgres:5432/sawtooth-bond-test";
        let conn = ConnectionPool::connect(&dsn).expect("Failed to connect to database");
        let manager = DataManager::new(&conn).expect("Failed to connect to database");
        EventHandler::new(manager)
    }

    fn make_block_commit_event(block_num: &str, block_id: &str) -> Event {
        let mut attr_1 = Event_Attribute::new();
        attr1.set_key(String::from("block_num"));
        attr1.set_value(String::from(block_num));

        let mut attr_2 = Event_Attribute::new();
        attr2.set_key(String::from("block_id"));
        attr2.set_value(String::from(block_id));

        let mut event = Event::new();
        event.set_event_type(String::from("sawtooth/block-commit"));
        event.set_attributes(protobuf::repeated::RepeatedField::from_vec(vec![attr1, attr2]));

        event
    }

    fn make_state_delta_event(data: &[u8]) -> Event {
        let mut event = Event::new();
        event.set_event_type(String::from("sawtooth/state-delta"));
        event.set_data(data);

        event
    }

    fn make_state_change(address: &str, value: &[u8], state_change_type: StateChange_Type) -> StateChange {
        let mut state_change = StateChange::new();
        state_change.set_address(String::from(address));
        state_change.set_value(value);
        state_change.set_field_type(state_change_type);
    }


    //protobuf::Message::write_to_bytes(policy_list).map_err(|err

    #[test]
    fn test_parse_block() {
        let event_handler = get_event_handler();

        let b = Block {
            block_num: 1,
            block_id: "block_1"
        };

        let block_commit_event = make_block_commit_event(b.block_num, b.block_id);

        let b2 = event_handler.parse_block(vec![block_commit_event])?;

        assert_eq!(b.block_num, b2.block_num);
        assert_eq!(b.block_id, b2.block_id);

    }
}

//TODO write Tests
//TODO maybe remove unessary options on new models? ... go to deal with missing fields in the proto messages.
