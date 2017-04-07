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
extern crate walkdir;

use std::path::Path;
use chrono::{DateTime, UTC};
use std::collections::HashMap;
use counter::{file_handling, record_handling};
use std::io::Write;
use std::sync::mpsc;
use walkdir::DirEntry;

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
            let mut agg: HashMap<record_handling::AggregateELBRecord, i64> = HashMap::new();
            debug!("Found {} files.", num_files);
            let mut filename_senders = Vec::new();
            let (agg_sender, agg_receiver) = mpsc::channel::<_>();
            let pool = sp::Pool::new(num_cpus::get());
            for sender_id in 0..pool.workers() {
                let (filename_sender, filename_receiver) = mpsc::channel::<_>();
                filename_senders.push(filename_sender);
                let cloned_agg_sender = agg_sender.clone();
                pool.spawn(move || {
                               run_file_processor(sender_id, &filename_receiver, &cloned_agg_sender)
                           });
            }

            let mut number_of_records = 0;
            let mut dones = 0;
            while dones < pool.workers() {
                match agg_receiver.recv() {
                    Ok(AggregationMessages::Start(sender_id)) => {
                        let sender = &filename_senders[sender_id];
                        if let Some(filename) = filenames.pop() {
                            let _ = sender.send(ParsingMessages::Filename(filename));
                        } else {
                            let _ = sender.send(ParsingMessages::Done);
                        }
                    }
                    Ok(AggregationMessages::Aggregate(num_parsed_records, new_agg, sender_id)) => {
                        debug!("Received new_agg having {} records.", new_agg.len());
                        let sender = &filename_senders[sender_id];
                        if let Some(filename) = filenames.pop() {
                            let _ = sender.send(ParsingMessages::Filename(filename));
                        } else {
                            let _ = sender.send(ParsingMessages::Done);
                        }
                        number_of_records += num_parsed_records;
                        record_handling::merge_aggregates(&new_agg, &mut agg);
                    }
                    Ok(AggregationMessages::Done) => dones += 1,
                    Err(_) => debug!("Received an error from one of the parsing workers."),
                }
            }

            debug!("Processed {} records in {} files.",
                   number_of_records,
                   num_files);

            for (aggregate, total) in &agg {
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
                         number_of_records,
                         time.num_milliseconds(),
                         agg.len());
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

enum AggregationMessages {
    Start(usize),
    Aggregate(usize, HashMap<record_handling::AggregateELBRecord, i64>, usize),
    Done,
}

enum ParsingMessages {
    Filename(DirEntry),
    Done,
}

// TODO: Test this.
// TODO: Use a real file.
fn run_file_processor(id: usize,
                      filename_receiver: &mpsc::Receiver<ParsingMessages>,
                      aggregate_sender: &mpsc::Sender<AggregationMessages>)
                      -> () {
    // TODO: There needs to be a timeout here to ensure the program doesn't run forever.
    // TODO: Make use of try_rec.
    // TODO: Report a timeout back to main.
    let _ = aggregate_sender.send(AggregationMessages::Start(id));
    loop {
        match filename_receiver.recv() {
            Ok(ParsingMessages::Filename(filename)) => {
                debug!("Received filename {}.", filename.path().display());
                if let Ok(file_aggregation_result) = file_handling::process_file(filename.path()) {
                    debug!("Found {} aggregates in {}.",
                           file_aggregation_result.aggregation.len(),
                           filename.path().display());

                    let _ = aggregate_sender.send(
                        AggregationMessages::Aggregate(
                            file_aggregation_result.num_raw_records,
                            file_aggregation_result.aggregation, id
                        )
                    );
                } else {
                    // TODO: Write the error to stderr.
                }
            }
            Ok(ParsingMessages::Done) |
            Err(_) => break,
        }
    }
    let _ = aggregate_sender.send(AggregationMessages::Done);
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
