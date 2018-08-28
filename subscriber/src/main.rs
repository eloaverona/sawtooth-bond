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

#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate sawtooth_sdk;
extern crate bond_database;
extern crate bond_common;
extern crate protobuf;
extern crate regex;
extern crate uuid;
use bond_database::{
    custom_types::*, connection_pool::ConnectionPool, models::*, tables_schema::*,
    data_manager::{DataManager, TransactionType}
};

use log::LogLevel;
pub mod subscriber;
pub mod event_handler;
use subscriber::Subscriber;
use event_handler::EventHandler;



fn main() {
    let matches = clap_app!(sawtooth_bond_subscriber =>
        (version: crate_version!())
        (about: "Sawtooth Bond Subscriber")
        (@arg connect: -C --connect +takes_value
           "connection endpoint for validator")
        (@arg verbose: -v --verbose +multiple
           "increase output verbosity")
        (@arg dbname: default_value("sawtooth-bond") --dbname +takes_value
           "the name of the database")
        (@arg dbhost: default_value("localhost") --dbhost +takes_value
            "the host of the database")
        (@arg dbport: default_value("5432") --dbport +takes_value
            "the port of the database")
        (@arg dbuser: default_value("sawtooth") --dbuser +takes_value
            "the authorized user of the database")
        (@arg dbpass: default_value("sawtooth") --dbpass +takes_value
            "the authorized user's password for database access"))
        .get_matches();

    let logger = match matches.occurrences_of("verbose") {
        1 => simple_logger::init_with_level(LogLevel::Info),
        2 => simple_logger::init_with_level(LogLevel::Debug),
        0 | _ => simple_logger::init_with_level(LogLevel::Warn),
    };

    let dsn = format!(
        "{}:{}@{}:{}/{}",
        matches.value_of("dbuser").unwrap(),
        matches.value_of("dbpass").unwrap(),
        matches.value_of("dbhost").unwrap(),
        matches.value_of("dbport").unwrap(),
        matches.value_of("dbname").unwrap()
    );

    let conn = ConnectionPool::connect(&dsn).expect("Failed to connect to database");
    info!("Successfully connected to database");
    let manager = DataManager::new(&conn).expect("Failed to connect to database");
    let event_handler = EventHandler::new(manager);
    let mut subscriber = Subscriber::new("tcp://192.168.200.192:4010", event_handler);
    subscriber.start(vec![String::from("0000000000000000")]).expect("error zmq ");
    //subscriber.stop();

}
