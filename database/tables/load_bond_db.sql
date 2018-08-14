
-- Create custom types

CREATE TYPE AssetType AS ENUM ('CURRENCY', 'BOND');
CREATE TYPE QuoteStatus AS ENUM ('OPEN', 'CLOSED');
CREATE TYPE OrderStatus AS ENUM ('OPEN', 'MATCHED', 'SETTLED');
CREATE TYPE OrderType AS ENUM ('MARKET', 'LIMIT');
CREATE TYPE OrderAction AS ENUM ('BUY', 'SELL');
CREATE TYPE OrganizationType AS ENUM ('TRADING_FIRM', 'PRICING_SOURCE');
CREATE TYPE Role AS ENUM ('MAKERTMAKER', 'TRADER');
CREATE TYPE PaymentType AS ENUM ('COUPON', 'REDEMPTION');
CREATE TYPE CouponType AS ENUM ('FIXED', 'FLOATING');
CREATE TYPE CouponFrequency AS ENUM ('QUARTERLY', 'MONTHLY', 'DAILY');


-- Create tables

CREATE TABLE IF NOT EXISTS chain_record (
  id                        bigserial   PRIMARY KEY,
  start_block_num           bigint,
  end_block_num             bigint
);


CREATE TABLE IF NOT EXISTS bonds (
  bond_id                   varchar,
  issuing_organization_id   varchar,
  amount_outstanding        bigint,
  coupon_rate               bigint,
  first_coupon_date         bigint,
  first_settlement_date     bigint,
  maturity_date             bigint,
  face_value                bigint,
  coupon_type               CouponType,
  coupon_frequency          CouponFrequency
) INHERITS (chain_record);

CREATE TABLE IF NOT EXISTS corporate_debt_ratings (
  bond_id                   varchar,
  agency                    varchar,
  rating                    varchar
) INHERITS (chain_record);

CREATE TABLE IF NOT EXISTS organizations (
  organization_id           varchar,
  industry                  varchar,
  name                      varchar,
  organization_type         OrganizationType
) INHERITS (chain_record);

CREATE TABLE IF NOT EXISTS authorizations (
  participant_public_key    varchar,
  organization_id           varchar,
  role                      Role
) INHERITS (chain_record);

CREATE TABLE IF NOT EXISTS receipts (
  payee_organization_id     varchar,
  bond_id                   varchar,
  payment_type              PaymentType,
  coupon_date               bigint,
  amount                    bigint,
  timestamp                 bigint
) INHERITS (chain_record);

CREATE TABLE IF NOT EXISTS holdings (
  owner_organization_id     varchar,
  asset_id                  varchar,
  asset_type                AssetType,
  amount                    bigint,
  address                   varchar
) INHERITS (chain_record);

CREATE TABLE IF NOT EXISTS participants (
  public_key                varchar,
  organization_id           varchar,
  username                  varchar
) INHERITS (chain_record);

CREATE TABLE IF NOT EXISTS auth (
  username                  varchar  PRIMARY KEY,
  hashed_password           varchar,
  encrypted_private_key     varchar
);

CREATE TABLE IF NOT EXISTS quotes (
  bond_id                   varchar,
  organization_id           varchar,
  ask_price                 bigint,
  ask_qty                   bigint,
  bid_price                 bigint,
  bid_qty                   bigint,
  quote_id                  varchar,
  status                    QuoteStatus,
  timestamp                 bigint
) INHERITS (chain_record);

CREATE TABLE IF NOT EXISTS orders (
  bond_id                   varchar,
  ordering_organization_id  varchar,
  order_id                  varchar,
  limit_price               bigint,
  limit_yield               bigint,
  quantity                  bigint,
  quote_id                  varchar,
  status                    OrderStatus,
  action                    OrderAction,
  order_type                OrderType,
  timestamp                 bigint
) INHERITS (chain_record);


CREATE TABLE IF NOT EXISTS settlements (
  order_id                  varchar,
  ordering_organization_id  varchar,
  quoting_organization_id   varchar,
  action                    OrderAction,
  bond_quantity             bigint,
  currency_amount           bigint
) INHERITS (chain_record);

CREATE TABLE IF NOT EXISTS blocks (
  block_num                bigint   PRIMARY KEY,
  block_id                 varchar
);
