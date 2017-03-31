use std::collections::HashMap;
use std::io::Write;

use chrono::{Date, DateTime, UTC};
use std::net::Ipv4Addr;
use regex::Regex;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AggregateELBRecord {
    pub day: Date<UTC>,
    pub client_address: Ipv4Addr,
    pub system_name: String,
}

impl AggregateELBRecord {
    fn new(day: DateTime<UTC>, client_address: Ipv4Addr, system: String) -> AggregateELBRecord {
        AggregateELBRecord {
            day: day.date(),
            client_address: client_address,
            system_name: system,
        }
    }
}

pub fn handle_parsing_result(counter_result: ::CounterResult,
                             aggregation: &mut HashMap<AggregateELBRecord, i64>)
                             -> () {
    match counter_result {
        Ok(elb_record) => {
            let aer =
                AggregateELBRecord::new(elb_record.timestamp,
                                        *elb_record.client_address.ip(),
                                        parse_system_name_regex(elb_record.request_url)
                                            .unwrap_or_else(|| "UNDEFINED_SYSTEM".to_owned()));
            aggregate_record(aer, aggregation);
        }
        Err(::CounterError::RecordParsingErrors(ref errs)) => println_stderr!("{:?}", errs.record),
        Err(ref err) => println_stderr!("{:?}", err),
    }
}

lazy_static! {
    static ref SYSTEM_REGEX: Regex = Regex::new(r"(?i)system=([^&]*)").unwrap();
}

fn parse_system_name_regex(q: &str) -> Option<String> {
    SYSTEM_REGEX.captures(q).and_then(|cap| cap.get(1).map(|sys| sys.as_str().to_string()))
}

pub fn aggregate_records(new_aggs: &HashMap<AggregateELBRecord, i64>,
                         aggregation: &mut HashMap<AggregateELBRecord, i64>)
                         -> () {
    for (agg_key, agg_val) in new_aggs {
        let total = aggregation.entry(agg_key.clone()).or_insert(0);
        *total += *agg_val;
    }
}

fn aggregate_record(aggregate_record: AggregateELBRecord,
                    aggregation: &mut HashMap<AggregateELBRecord, i64>)
                    -> () {
    let total = aggregation.entry(aggregate_record).or_insert(0);
    *total += 1;
}

#[cfg(test)]
mod aggregate_records_tests {

    use std::collections::HashMap;
    use super::AggregateELBRecord;
    use super::aggregate_record;
    use chrono::{DateTime, UTC};
    use std::net::SocketAddrV4;

    #[test]
    fn inserting_two_records_with_different_values_creates_two_entries_each_recorded_once() {
        let mut agg: HashMap<AggregateELBRecord, i64> = HashMap::new();

        let ar0 = AggregateELBRecord {
            day: "2015-08-15T23:43:05.302180Z".parse::<DateTime<UTC>>().unwrap().date(),
            client_address: *"172.16.1.6:54814".parse::<SocketAddrV4>().unwrap().ip(),
            system_name: "sys1".to_owned(),
        };

        let ar1 = AggregateELBRecord {
            day: "2015-08-15T23:43:05.302180Z".parse::<DateTime<UTC>>().unwrap().date(),
            client_address: *"172.16.1.6:54814".parse::<SocketAddrV4>().unwrap().ip(),
            system_name: "sys2".to_owned(),
        };

        aggregate_record(ar0, &mut agg);
        aggregate_record(ar1, &mut agg);

        assert_eq!(agg.len(), 2);
        for (_, total) in agg {
            assert_eq!(total, 1)
        }
    }

    #[test]
    fn inserting_two_records_with_the_same_values_increases_the_total_correctly() {
        let mut agg: HashMap<AggregateELBRecord, i64> = HashMap::new();

        let ar0 = AggregateELBRecord {
            day: "2015-08-15T23:43:05.302180Z".parse::<DateTime<UTC>>().unwrap().date(),
            client_address: *"172.16.1.6:54814".parse::<SocketAddrV4>().unwrap().ip(),
            system_name: "sys1".to_owned(),
        };

        let ar1 = ar0.clone();
        let ar3 = ar0.clone();

        aggregate_record(ar0, &mut agg);
        aggregate_record(ar1, &mut agg);

        assert_eq!(agg[&ar3], 2);
    }
}
