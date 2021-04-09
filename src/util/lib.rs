#![allow(clippy::upper_case_acronyms)]
//! Crate containing common functions and types used in Garage

#[macro_use]
extern crate log;

pub mod background;
pub mod config;
pub mod data;
pub mod error;
pub mod persister;
pub mod time;
