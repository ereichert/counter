extern crate rustc_serialize;
extern crate elp;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate clap;
extern crate chrono;
#[macro_use]
extern crate counter;
extern crate num_cpus;
extern crate scoped_pool as sp;

use std::path::Path;
use chrono::{DateTime, UTC};
use std::collections::HashMap;
use counter::{file_handling, record_handling};
use counter::file_handling::{AggregationMessages, FileHandlingMessages};
use std::io::Write;
use std::sync::mpsc;

const EXIT_SUCCESS: i32 = 0;
const EXIT_FAILURE: i32 = 1;

// TODO: Refactor and test this code.
fn main() {
    env_logger::init().unwrap();
    let runtime_context = RuntimeContext::new();
    let log_location = runtime_context.log_location();

    debug!("Running summary on {}.", log_location.to_str().unwrap());

    let start: Option<DateTime<UTC>> = if runtime_context.run_benchmark() {
        Some(UTC::now())
    } else {
        None
    };

    let exit_code = match file_handling::file_list(log_location) {
        Ok(ref mut filenames) => {
            let num_files = filenames.len();
            let mut final_agg = HashMap::new();
            let mut number_of_raw_records = 0;
            debug!("Found {} files.", num_files);
            let mut file_handling_msg_senders = Vec::new();
            let (agg_msg_sender, agg_msg_receiver) = mpsc::channel::<_>();
            let pool = sp::Pool::new(num_cpus::get());
            for sender_id in 0..pool.workers() {
                let (file_handling_msg_sender, file_handling_msg_receiver) = mpsc::channel::<_>();
                file_handling_msg_senders.push(file_handling_msg_sender);
                let cloned_agg_msg_sender = agg_msg_sender.clone();
                pool.spawn(move || {
                               file_handling::FileAggregator::new(sender_id)
                                   .run(&file_handling_msg_receiver, &cloned_agg_msg_sender);
                           });
            }

            let mut remaining_workers = pool.workers();
            while remaining_workers > 0 {
                match agg_msg_receiver.recv() {
                    Ok(AggregationMessages::Next(sender_id)) => {
                        let sender = &file_handling_msg_senders[sender_id];
                        if let Some(filename) = filenames.pop() {
                            let _ = sender.send(FileHandlingMessages::Filename(filename));
                        } else {
                            let _ = sender.send(FileHandlingMessages::Done);
                        }
                    }
                    Ok(AggregationMessages::Aggregate(num_parsed_records, new_agg)) => {
                        debug!("Received new_agg having {} records.", new_agg.len());
                        number_of_raw_records += num_parsed_records;
                        record_handling::merge_aggregates(&new_agg, &mut final_agg);
                        remaining_workers -= 1
                    }
                    Err(_) => debug!("Received an error from one of the parsing workers."),
                }
            }

            debug!("Processed {} records in {} files.",
                   number_of_raw_records,
                   num_files);

            for (aggregate, total) in &final_agg {
                println!("{},{},{},{}",
                         aggregate.system_name,
                         aggregate.day.format("%Y-%m-%d").to_string(),
                         aggregate.client_address,
                         total);
            }

            if let Some(start_time) = start {
                let end_time = UTC::now();
                let time = end_time - start_time;
                println!("Processed {} files having {} records in {} milliseconds and produced \
                          {} aggregates.",
                         num_files,
                         number_of_raw_records,
                         time.num_milliseconds(),
                         final_agg.len());
            }
            pool.shutdown();
            EXIT_SUCCESS
        }

        Err(e) => {
            println_stderr!("The following error occurred while trying to get the list of files. \
                             {}",
                            e);
            EXIT_FAILURE
        }
    };

    std::process::exit(exit_code);
}

const LOG_LOCATION_ARG: &'static str = "log-location";
const BENCHMARK_ARG: &'static str = "benchmark";

struct RuntimeContext<'a> {
    arg_matches: clap::ArgMatches<'a>,
}

impl<'a> RuntimeContext<'a> {
    fn new() -> RuntimeContext<'a> {
        let arg_matches = RuntimeContext::new_app().get_matches();

        RuntimeContext { arg_matches: arg_matches }
    }

    #[cfg(test)]
    fn new_test_runtime_context(args: Vec<&str>) -> RuntimeContext<'a> {
        let arg_matches = RuntimeContext::new_app()
            .get_matches_from_safe_borrow(args)
            .unwrap();

        RuntimeContext { arg_matches: arg_matches }
    }

    fn new_app<'b>() -> clap::App<'a, 'b> {
        clap::App::new("counter")
            .arg(clap::Arg::with_name(LOG_LOCATION_ARG)
                     .required(true)
                     .help("The root directory when the log files are stored."))
            .arg(clap::Arg::with_name(BENCHMARK_ARG)
                     .required(false)
                     .help("Time the run and provide statistics at the end of the run.")
                     .long("benchmark")
                     .short("b"))
    }

    fn run_benchmark(&self) -> bool {
        self.arg_matches.is_present(BENCHMARK_ARG)
    }

    fn log_location(&self) -> &Path {
        Path::new(self.arg_matches.value_of(LOG_LOCATION_ARG).unwrap())
    }
}

#[cfg(test)]
mod runtime_context_tests {
    use super::*;
    use std::panic;

    #[test]
    fn log_location_should_return_the_specified_value() {
        let arg_vec = vec!["counter", "~/logs"];

        let runtime_context = RuntimeContext::new_test_runtime_context(arg_vec);

        assert_eq!(runtime_context.log_location().to_str().unwrap(), "~/logs")
    }

    #[test]
    fn constructing_a_runtime_context_should_not_panic_if_the_log_location_is_specified() {
        let arg_vec = vec!["counter", "~/logs"];

        let result = panic::catch_unwind(|| { RuntimeContext::new_test_runtime_context(arg_vec); });

        assert!(result.is_ok())
    }

    #[test]
    fn constructing_a_runtime_context_should_panic_if_the_log_location_is_not_specified() {
        let arg_vec = vec!["counter"];

        let result = panic::catch_unwind(|| { RuntimeContext::new_test_runtime_context(arg_vec); });

        assert!(result.is_err())
    }

    #[test]
    fn run_benchmark_should_return_false_when_benchmark_arg_is_not_set() {
        let arg_vec = vec!["counter", "~/logs"];

        let runtime_context = RuntimeContext::new_test_runtime_context(arg_vec);

        assert_eq!(runtime_context.run_benchmark(), false)
    }

    #[test]
    fn run_benchmark_should_return_true_when_long_benchmark_is_set() {
        let arg_vec = vec!["counter", "--benchmark", "~/logs"];

        let runtime_context = RuntimeContext::new_test_runtime_context(arg_vec);

        assert!(runtime_context.run_benchmark())
    }

    #[test]
    fn run_benchmark_should_return_true_when_short_benchmark_is_set() {
        let arg_vec = vec!["counter", "-b", "~/logs"];

        let runtime_context = RuntimeContext::new_test_runtime_context(arg_vec);

        assert!(runtime_context.run_benchmark())
    }
}
