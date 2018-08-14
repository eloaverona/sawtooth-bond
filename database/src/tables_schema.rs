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

table! {
    auth (username) {
        username -> Varchar,
        hashed_password -> Nullable<Varchar>,
        encrypted_private_key -> Nullable<Varchar>,
    }
}

table! {
    use diesel::sql_types::*;
    use super::Role;
    authorizations (id) {
        id -> Int8,
        participant_public_key -> Nullable<Varchar>,
        organization_id -> Nullable<Varchar>,
        role -> Nullable<Role>,
        start_block_num -> Nullable<Int8>,
        end_block_num -> Nullable<Int8>,
    }
}

table! {
    blocks (block_num) {
        block_num -> Int8,
        block_id -> Nullable<Varchar>,
    }
}

table! {
    use diesel::sql_types::*;
    use super::CouponFrequency;
    use super::CouponType;
    bonds (id) {
        id -> Int8,
        bond_id -> Nullable<Varchar>,
        issuing_organization_id -> Nullable<Varchar>,
        amount_outstanding -> Nullable<Int8>,
        coupon_rate -> Nullable<Int8>,
        first_coupon_date -> Nullable<Int8>,
        first_settlement_date -> Nullable<Int8>,
        maturity_date -> Nullable<Int8>,
        face_value -> Nullable<Int8>,
        coupon_type -> Nullable<CouponType>,
        coupon_frequency -> Nullable<CouponFrequency>,
        start_block_num -> Nullable<Int8>,
        end_block_num -> Nullable<Int8>,
    }
}

table! {
    corporate_debt_ratings (id) {
        id -> Int8,
        bond_id -> Nullable<Varchar>,
        agency -> Nullable<Varchar>,
        rating -> Nullable<Varchar>,
        start_block_num -> Nullable<Int8>,
        end_block_num -> Nullable<Int8>,
    }
}

table! {
    use diesel::sql_types::*;
    use super::AssetType;
    holdings (id) {
        id -> Int8,
        owner_organization_id -> Nullable<Varchar>,
        asset_id -> Nullable<Varchar>,
        asset_type -> Nullable<AssetType>,
        amount -> Nullable<Int8>,
        start_block_num -> Nullable<Int8>,
        end_block_num -> Nullable<Int8>,
        address -> Nullable<Varchar>,
    }
}

table! {
    use diesel::sql_types::*;
    use super::OrderStatus;
    use super::OrderAction;
    use super::OrderType;
    orders (id) {
        id -> Int8,
        bond_id -> Nullable<Varchar>,
        ordering_organization_id -> Nullable<Varchar>,
        order_id -> Nullable<Varchar>,
        limit_price -> Nullable<Int8>,
        limit_yield -> Nullable<Int8>,
        quantity -> Nullable<Int8>,
        quote_id -> Nullable<Varchar>,
        status -> Nullable<OrderStatus>,
        action -> Nullable<OrderAction>,
        order_type -> Nullable<OrderType>,
        timestamp -> Nullable<Int8>,
        start_block_num -> Nullable<Int8>,
        end_block_num -> Nullable<Int8>,
    }
}

table! {
    use diesel::sql_types::*;
    use super::OrganizationType;
    organizations (id) {
        id -> Int8,
        organization_id -> Nullable<Varchar>,
        industry -> Nullable<Varchar>,
        name -> Nullable<Varchar>,
        organization_type -> Nullable<OrganizationType>,
        start_block_num -> Nullable<Int8>,
        end_block_num -> Nullable<Int8>,
    }
}

table! {
    chain_record (id) {
        id -> Int8,
        start_block_num -> Nullable<Int8>,
        end_block_num -> Nullable<Int8>,
    }
}

table! {
    participants (id) {
        id -> Int8,
        public_key -> Nullable<Varchar>,
        organization_id -> Nullable<Varchar>,
        username -> Nullable<Varchar>,
        start_block_num -> Nullable<Int8>,
        end_block_num -> Nullable<Int8>,
    }
}

table! {
    use diesel::sql_types::*;
    use super::QuoteStatus;
    quotes (id) {
        id -> Int8,
        bond_id -> Nullable<Varchar>,
        organization_id -> Nullable<Varchar>,
        ask_price -> Nullable<Int8>,
        ask_qty -> Nullable<Int8>,
        bid_price -> Nullable<Int8>,
        bid_qty -> Nullable<Int8>,
        quote_id -> Nullable<Varchar>,
        status -> Nullable<QuoteStatus>,
        timestamp -> Nullable<Int8>,
        start_block_num -> Nullable<Int8>,
        end_block_num -> Nullable<Int8>,
    }
}

table! {
    use diesel::sql_types::*;
    use super::PaymentType;
    receipts (id) {
        id -> Int8,
        payee_organization_id -> Nullable<Varchar>,
        bond_id -> Nullable<Varchar>,
        payment_type -> Nullable<PaymentType>,
        coupon_date -> Nullable<Int8>,
        amount -> Nullable<Int8>,
        timestamp -> Nullable<Int8>,
        start_block_num -> Nullable<Int8>,
        end_block_num -> Nullable<Int8>,
    }
}

table! {
    use diesel::sql_types::*;
    use super::OrderAction;
    settlements (id) {
        id -> Int8,
        order_id -> Nullable<Varchar>,
        ordering_organization_id -> Nullable<Varchar>,
        quoting_organization_id -> Nullable<Varchar>,
        action -> Nullable<OrderAction>,
        bond_quantity -> Nullable<Int8>,
        currency_amount -> Nullable<Int8>,
        start_block_num -> Nullable<Int8>,
        end_block_num -> Nullable<Int8>,
    }
}

allow_tables_to_appear_in_same_query!(
    auth,
    authorizations,
    blocks,
    bonds,
    corporate_debt_ratings,
    holdings,
    orders,
    organizations,
    participants,
    quotes,
    receipts,
    settlements,
);
