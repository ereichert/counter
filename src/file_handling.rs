use std::path::Path;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use walkdir;
use walkdir::{DirEntry, WalkDir};
use elp;
use {ELBRecordAggregation, FileAggregation, CounterError};
use std::collections::HashMap;
use record_handling;
use std::io::Write;
use std::sync::mpsc;
use std::time::Duration;

pub fn file_list(dir: &Path) -> Result<Vec<DirEntry>, walkdir::Error> {
    let mut filenames = Vec::new();
    let dir_entries = WalkDir::new(dir);
    for entry in dir_entries {
        let dir_entry = entry?;
        if dir_entry
               .path()
               .extension()
               .map(|ext| ext.eq("log"))
               .unwrap_or(false) {
            filenames.push(dir_entry);
        }
    }

    Ok(filenames)
}

pub enum AggregationMessages {
    Aggregate(usize, ELBRecordAggregation),
    Next(usize),
}

pub enum FileHandlingMessages {
    Filename(DirEntry),
    Done,
}

pub struct FileAggregator {
    id: usize,
    num_raw_records: usize,
    final_agg: ELBRecordAggregation,
}

impl FileAggregator {
    pub fn new(id: usize) -> FileAggregator {
        FileAggregator {
            id: id,
            num_raw_records: 0,
            final_agg: HashMap::new(),
        }
    }

    pub fn run(mut self,
               filename_receiver: &mpsc::Receiver<FileHandlingMessages>,
               aggregate_sender: &mpsc::Sender<AggregationMessages>)
               -> () {
        let _ = aggregate_sender.send(AggregationMessages::Next(self.id));
        let timeout = Duration::from_millis(10000);
        loop {
            match filename_receiver.recv_timeout(timeout) {
                Ok(FileHandlingMessages::Filename(filename)) => {
                    debug!("Received filename {}.", filename.path().display());
                    self.aggregate_file(&filename);
                    let _ = aggregate_sender.send(AggregationMessages::Next(self.id));
                }
                Ok(FileHandlingMessages::Done) => break,
                Err(_) => break,// TODO: Send error to main
            }
        }

        let _ = aggregate_sender.send(AggregationMessages::Aggregate(self.num_raw_records,
                                                                     self.final_agg));
    }

    fn aggregate_file(&mut self, filename: &DirEntry) -> () {
        if let Ok(file_aggregation_result) = self.process_file(filename.path()) {
            debug!("Found {} aggregates in {}.",
                   file_aggregation_result.aggregation.len(),
                   filename.path().display());
            self.num_raw_records += file_aggregation_result.num_raw_records;
            record_handling::merge_aggregates(&file_aggregation_result.aggregation,
                                              &mut self.final_agg);
        } else {
            println_stderr!("Failed to read file {}.", filename.path().display());
        }
    }

    fn process_file(&self, path: &Path) -> Result<FileAggregation, io::Error> {
        debug!("Processing file {}.", path.display());
        match File::open(path) {
            Ok(file) => {
                let aggregation_result = self.read_records(path, &file);
                debug!("Found {} records in file {}.",
                       aggregation_result.num_raw_records,
                       path.display());
                Ok(aggregation_result)
            }

            Err(err) => Err(err),
        }
    }

    fn read_records(&self, path: &Path, file: &File) -> FileAggregation {
        let mut agg: ELBRecordAggregation = HashMap::new();
        let mut file_record_count = 0;
        for possible_record in BufReader::new(file).lines() {
            file_record_count += 1;
            if let Ok(record) = possible_record {
                let parsing_result = elp::parse_record(&record)
                    .map_err(CounterError::RecordParsingErrors);
                record_handling::handle_parsing_result(parsing_result, &mut agg);
            } else {
                println_stderr!("Failed to read line {} in file {}.",
                                file_record_count + 1,
                                path.display());
            }
        }

        FileAggregation {
            num_raw_records: file_record_count,
            aggregation: agg,
        }
    }
}

#[cfg(test)]
mod file_aggregator_run_tests {

    use std::sync::mpsc;
    use test_common::TEST_LOG_FILE;

    #[test]
    fn sends_the_final_agg_after_receiving_the_done_message() {
        let (filename_sender, filename_receiver) = mpsc::channel();
        let (agg_sender, agg_receiver) = mpsc::channel();
        let _ = filename_sender.send(super::FileHandlingMessages::Done);
        let file_aggregator = super::FileAggregator::new(1);
        file_aggregator.run(&filename_receiver, &agg_sender);

        // Dump the AggregationMessages::Next message sent at startup.
        let _ = agg_receiver.recv();

        match agg_receiver.recv().unwrap() {
            super::AggregationMessages::Aggregate(size, agg) => {
                assert_eq!(size, 0);
                assert_eq!(agg.len(), 0);
            }
            super::AggregationMessages::Next(_) => panic!("Received an unexpected Next message."),
        }
    }

    #[test]
    fn sends_a_next_message_after_starting_up() {
        let (filename_sender, filename_receiver) = mpsc::channel();
        let (agg_sender, agg_receiver) = mpsc::channel();
        let _ = filename_sender.send(super::FileHandlingMessages::Done);
        let file_aggregator = super::FileAggregator::new(1);

        file_aggregator.run(&filename_receiver, &agg_sender);

        match agg_receiver.recv().unwrap() {
            super::AggregationMessages::Next(id) => assert_eq!(id, 1),
            super::AggregationMessages::Aggregate(_, _) => panic!("Received an unexpected Aggregate message."),
        }
    }
}

#[cfg(test)]
mod file_aggregator_read_records {

    use std::fs::File;
    use std::path::Path;
    use test_common;

    #[test]
    fn read_records() {
        let path = Path::new(test_common::TEST_LOG_FILE);
        let file = File::open(&path).unwrap();
        let file_aggregator = super::FileAggregator::new(0);


        let returned_agg = file_aggregator.read_records(&path, &file);

        assert_eq!(returned_agg.aggregation.len(),
                   test_common::TEST_LOG_FILE_AGGS)
    }
}

#[cfg(test)]
mod file_aggregator_process_file_tests {
    use std::path::Path;
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use test_common;

    #[test]
    fn process_file_should_return_a_result_with_the_correct_number_of_processed_records() {
        let num_lines = BufReader::new(File::open(test_common::TEST_LOG_FILE).unwrap())
            .lines()
            .collect::<Vec<_>>()
            .len();
        let log_path = Path::new(test_common::TEST_LOG_FILE);
        let file_aggregator = super::FileAggregator::new(0);

        let file_agg_result = file_aggregator.process_file(&log_path).unwrap();

        assert_eq!(file_agg_result.num_raw_records, num_lines)
    }

    #[test]
    fn process_file_should_return_an_error_when_the_file_cannot_be_opened() {
        let log_path = Path::new("bad_filename");
        let file_aggregator = super::FileAggregator::new(0);

        let result = file_aggregator.process_file(&log_path);

        assert!(result.is_err())
    }
}

#[cfg(test)]
mod file_list_tests {
    extern crate names;
    extern crate rand;

    use std::{fs, panic, sync};
    use std::io::Write;
    use std::path::Path;
    use self::rand::distributions::{IndependentSample, Range};

    #[test]
    fn file_list_should_return_the_correct_number_of_files() {
        run_int_test_in_test_dir(|test_dir| {
            let mut thread_range = rand::thread_rng();
            let num_files_range = Range::new(5, 20);
            let num_files = num_files_range.ind_sample(&mut thread_range);
            let directory_depth_range = Range::new(1, 6);

            for _ in 0..num_files {
                let directory_depth = directory_depth_range.ind_sample(&mut thread_range);
                let mut path = test_dir.to_owned();
                for _ in 0..directory_depth {
                    path.push_str(format!("/{}", NAME_GENERATOR.next()).as_str());
                    let _ = fs::create_dir(&path);
                }

                path.push_str(format!("/{}.log", NAME_GENERATOR.next()).as_str());
                let _ = fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&path)
                    .unwrap()
                    .write(&"test content".as_bytes());
            }

            let files = super::file_list(Path::new(test_dir)).unwrap();

            assert_ne!(num_files, 0);
            assert_eq!(files.len(), num_files)
        })
    }

    #[test]
    fn file_list_should_return_0_when_there_are_no_files_in_the_directory() {
        run_int_test_in_test_dir(|test_dir| {
                                     let files = super::file_list(Path::new(test_dir)).unwrap();

                                     assert_eq!(files.len(), 0)
                                 })
    }

    pub fn run_int_test_in_test_dir<T>(test: T) -> ()
        where T: FnOnce(&str) -> () + panic::UnwindSafe
    {
        creat_test_dir();
        let test_dir_path = create_empty_dir();

        let result = panic::catch_unwind(|| test(&test_dir_path));

        let _ = fs::remove_dir_all(&test_dir_path);

        if let Err(err) = result {
            panic::resume_unwind(err);
        }
    }

    pub const TEST_DIR: &'static str = "./int_tests";
    static TEST_DIR_SYNC: sync::Once = sync::ONCE_INIT;
    fn creat_test_dir() {
        TEST_DIR_SYNC.call_once(|| { let _ = fs::create_dir(TEST_DIR); });
    }

    fn create_empty_dir() -> String {
        let path = format!("{}/{}", TEST_DIR, NAME_GENERATOR.next());
        let _ = fs::create_dir(&path);
        path
    }

    struct NameGenerator {
        generator: Option<names::Generator<'static>>,
    }

    impl NameGenerator {
        fn next(&mut self) -> String {
            if self.generator.is_none() {
                self.generator = Some(names::Generator::default());
            }

            self.generator
                .as_mut()
                .map(|mut g| g.next().unwrap())
                .unwrap()
        }
    }

    const NAME_GENERATOR: NameGenerator = NameGenerator { generator: None };
}
