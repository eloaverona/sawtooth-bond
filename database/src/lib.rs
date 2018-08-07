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

//allow compile warning that can is caused by diesel and can only be fixed on diesel
#![allow(proc_macro_derive_resolution_fallback)]

pub mod connection_pool;
pub mod custom_types;
pub mod data_manager;
pub mod errors;
pub mod models;
pub mod tables_schema;

#[macro_use]
extern crate diesel;

extern crate r2d2;
extern crate r2d2_diesel;
