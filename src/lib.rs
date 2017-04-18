extern crate walkdir;
#[macro_use]
extern crate log;
extern crate elp;
extern crate chrono;
extern crate regex;
#[macro_use]
extern crate lazy_static;
extern crate scoped_pool as sp;
extern crate num_cpus;

use std::fmt;
use std::fmt::{Display, Formatter};
use std::error::Error;
use std::collections::HashMap;

#[macro_export]
macro_rules! println_stderr(
    ($($arg:tt)*) => (
        match writeln!(&mut ::std::io::stderr(), $($arg)* ) {
            Ok(_) => {},
            Err(x) => panic!("Unable to write to stderr: {}", x),
        }
    )
);

pub mod file_handling;
pub mod record_handling;
pub mod aggregation_control;

pub type ELBRecordAggregation = HashMap<record_handling::AggregateELBRecord, i64>;
pub struct FileAggregation {
    pub num_raw_records: usize,
    pub aggregation: ELBRecordAggregation,
}
pub type CounterResult<'a> = Result<elp::ELBRecord<'a>, CounterError<'a>>;

#[derive(Debug, PartialEq)]
pub enum CounterError<'a> {
    RecordParsingErrors(elp::ParsingErrors<'a>),
}

impl<'a> Display for CounterError<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            CounterError::RecordParsingErrors(ref errs) => {
                write!(f, "Parsing errors: {:?}.", errs.errors)
            }
        }
    }
}

impl<'a> Error for CounterError<'a> {
    fn description(&self) -> &str {
        match *self {
            CounterError::RecordParsingErrors(_) => "failed to parse record",
        }
    }
}

#[cfg(test)]
pub mod test_common;
