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

extern crate postgres;
use postgres::{Connection, TlsMode, Error};

pub struct Database {
    dsn: String,
    conn: Option<Connection>
}

impl Database {
    pub fn new(dsn: &str) -> Database {
        Database {
            dsn: dsn.to_string(),
            conn: None
        }
    }

    pub fn create_tables(&self) -> Result<(), Error>{
        unimplemented!()
    }

    pub fn connect(&mut self) -> Result<(), Error> {
        let full_dsn = String::from("postgres://") + &self.dsn;
        self.conn = Some(Connection::connect(full_dsn, TlsMode::None)?);
        info!("Successfully connected to database");
        Ok(())
    }

    pub fn disconnect(&mut self) -> Result<(), Error>{
        if self.conn.is_some() {
            self.conn.take().unwrap().finish()?;

        }
        info!("Successfully disconnected from database");
        Ok(())
    }
}
