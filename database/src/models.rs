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

use custom_types::*;
use tables_schema::*;

#[derive(Queryable, Insertable)]
pub struct Block {
    pub block_num: i64,
    pub block_id: Option<String>,
}

#[derive(Insertable)]
#[table_name = "blocks"]
pub struct NewBlock {
    pub block_num: i64,
    pub block_id: Option<String>,
}

#[derive(Queryable)]
pub struct Authorization {
    pub id: i64,
    pub participant_public_key: Option<String>,
    pub organization_id: Option<String>,
    pub role: Option<RoleEnum>,
    pub start_block_num: Option<i64>,
    pub end_block_num: Option<i64>,
    pub address: Option<String>,
}

#[derive(Queryable, Insertable, Debug, PartialEq)]
#[table_name = "authorizations"]
pub struct NewAuthorization {
    pub participant_public_key: String,
    pub organization_id: String,
    pub role: RoleEnum,
    pub start_block_num: i64,
    pub end_block_num: i64,
    pub address: String,
}

#[derive(Queryable)]
pub struct Bond {
    pub id: i64,
    pub bond_id: Option<String>,
    pub issuing_organization_id: Option<String>,
    pub amount_outstanding: Option<i64>,
    pub coupon_rate: Option<i64>,
    pub first_coupon_date: Option<i64>,
    pub first_settlement_date: Option<i64>,
    pub maturity_date: Option<i64>,
    pub face_value: Option<i64>,
    pub coupon_type: Option<CouponTypeEnum>,
    pub coupon_frequency: Option<CouponFrequencyEnum>,
    pub start_block_num: Option<i64>,
    pub end_block_num: Option<i64>,
}

#[derive(Queryable, Insertable)]
#[table_name = "bonds"]
pub struct NewBond {
    pub bond_id: String,
    pub issuing_organization_id: String,
    pub amount_outstanding: i64,
    pub coupon_rate: i64,
    pub first_coupon_date: i64,
    pub first_settlement_date: i64,
    pub maturity_date: i64,
    pub face_value: i64,
    pub coupon_type: CouponTypeEnum,
    pub coupon_frequency: CouponFrequencyEnum,
    pub start_block_num: i64,
    pub end_block_num: i64,
}

#[derive(Queryable)]
pub struct CorporateDebtRating {
    pub id: i64,
    pub bond_id: Option<String>,
    pub agency: Option<String>,
    pub rating: Option<String>,
    pub start_block_num: Option<i64>,
    pub end_block_num: Option<i64>,
}

#[derive(Queryable, Insertable)]
#[table_name = "corporate_debt_ratings"]
pub struct NewCorporateDebtRating {
    pub bond_id: String,
    pub agency: String,
    pub rating: String,
    pub start_block_num: i64,
    pub end_block_num: i64,
}

#[derive(Queryable)]
pub struct Holding {
    pub id: i64,
    pub owner_organization_id: Option<String>,
    pub asset_id: Option<String>,
    pub asset_type: Option<AssetTypeEnum>,
    pub amount: Option<i64>,
    pub start_block_num: Option<i64>,
    pub end_block_num: Option<i64>,
    pub address: Option<String>,
}

#[derive(Queryable, Insertable)]
#[table_name = "holdings"]
pub struct NewHolding {
    pub owner_organization_id: String,
    pub asset_id: String,
    pub asset_type: AssetTypeEnum,
    pub amount: i64,
    pub start_block_num: i64,
    pub end_block_num: i64,
    pub address: String,
}

#[derive(Queryable)]
pub struct Order {
    pub id: i64,
    pub bond_id: Option<String>,
    pub ordering_organization_id: Option<String>,
    pub order_id: Option<String>,
    pub limit_price: Option<i64>,
    pub limit_yield: Option<i64>,
    pub quantity: Option<i64>,
    pub quote_id: Option<String>,
    pub status: Option<OrderStatusEnum>,
    pub action: Option<OrderActionEnum>,
    pub order_type: Option<OrderTypeEnum>,
    pub timestamp: Option<i64>,
    pub start_block_num: Option<i64>,
    pub end_block_num: Option<i64>,
}

#[derive(Queryable, Insertable)]
#[table_name = "orders"]
pub struct NewOrder {
    pub bond_id: String,
    pub ordering_organization_id: String,
    pub order_id: String,
    pub limit_price: Option<i64>,
    pub limit_yield: Option<i64>,
    pub quantity: i64,
    pub quote_id: Option<String>,
    pub status: OrderStatusEnum,
    pub action: OrderActionEnum,
    pub order_type: OrderTypeEnum,
    pub timestamp: i64,
    pub start_block_num: i64,
    pub end_block_num: i64,
}

#[derive(Queryable)]
pub struct Organization {
    pub id: i64,
    pub organization_id: Option<String>,
    pub industry: Option<String>,
    pub name: Option<String>,
    pub organization_type: Option<OrganizationTypeEnum>,
    pub start_block_num: Option<i64>,
    pub end_block_num: Option<i64>,
}

#[derive(Queryable, Insertable)]
#[table_name = "organizations"]
pub struct NewOrganization {
    pub organization_id: String,
    pub industry: Option<String>,
    pub name: Option<String>,
    pub organization_type: OrganizationTypeEnum,
    pub start_block_num: i64,
    pub end_block_num: i64,
}

#[derive(Queryable)]
pub struct Participant {
    pub id: i64,
    pub public_key: Option<String>,
    pub organization_id: Option<String>,
    pub username: Option<String>,
    pub start_block_num: Option<i64>,
    pub end_block_num: Option<i64>,
}

#[derive(Queryable, Insertable)]
#[table_name = "participants"]
pub struct NewParticipant {
    pub public_key: String,
    pub organization_id: String,
    pub username: String,
    pub start_block_num: i64,
    pub end_block_num: i64,
}

#[derive(Queryable)]
pub struct Auth {
    pub username: String,
    pub hashed_password: Option<String>,
    pub encrypted_private_key: Option<String>,
}

#[derive(Queryable, Insertable)]
#[table_name = "auth"]
pub struct NewAuth {
    pub username: String,
    pub hashed_password: String,
    pub encrypted_private_key: String,
}

#[derive(Queryable)]
pub struct Quote {
    pub id: i64,
    pub bond_id: Option<String>,
    pub organization_id: Option<String>,
    pub ask_price: Option<i64>,
    pub ask_qty: Option<i64>,
    pub bid_price: Option<i64>,
    pub bid_qty: Option<i64>,
    pub quote_id: Option<String>,
    pub status: Option<QuoteStatusEnum>,
    pub timestamp: Option<i64>,
    pub start_block_num: Option<i64>,
    pub end_block_num: Option<i64>,
}

#[derive(Queryable, Insertable)]
#[table_name = "quotes"]
pub struct NewQuote {
    pub bond_id: String,
    pub organization_id: String,
    pub ask_price: i64,
    pub ask_qty: i64,
    pub bid_price: i64,
    pub bid_qty: i64,
    pub quote_id: String,
    pub status: QuoteStatusEnum,
    pub timestamp: i64,
    pub start_block_num: i64,
    pub end_block_num: i64,
}

#[derive(Queryable)]
pub struct Settlement {
    pub id: i64,
    pub order_id: Option<String>,
    pub ordering_organization_id: Option<String>,
    pub quoting_organization_id: Option<String>,
    pub action: Option<OrderActionEnum>,
    pub bond_quantity: Option<i64>,
    pub currency_amount: Option<i64>,
    pub start_block_num: Option<i64>,
    pub end_block_num: Option<i64>,
}

#[derive(Queryable, Insertable)]
#[table_name = "settlements"]
pub struct NewSettlement {
    pub order_id: String,
    pub ordering_organization_id: String,
    pub quoting_organization_id: String,
    pub action: OrderActionEnum,
    pub bond_quantity: i64,
    pub currency_amount: i64,
    pub start_block_num: i64,
    pub end_block_num: i64,
}
#[derive(Queryable)]
pub struct Receipt {
    pub id: i64,
    pub payee_organization_id: Option<String>,
    pub bond_id: Option<String>,
    pub payment_type: Option<PaymentTypeEnum>,
    pub coupon_date: Option<i64>,
    pub amount: Option<i64>,
    pub timestamp: Option<i64>,
    pub start_block_num: Option<i64>,
    pub end_block_num: Option<i64>,
}

#[derive(Queryable, Insertable)]
#[table_name = "receipts"]
pub struct NewReceipt {
    pub payee_organization_id: String,
    pub bond_id: String,
    pub payment_type: PaymentTypeEnum,
    pub coupon_date: i64,
    pub amount: i64,
    pub timestamp: i64,
    pub start_block_num: i64,
    pub end_block_num: i64,
}
