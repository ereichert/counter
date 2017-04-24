use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use walkdir;
use walkdir::WalkDir;
use ELBRecordAggregation;
use std::collections::HashMap;
use record_handling;
use std::io::Write;
use std::sync::mpsc;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;
use std::path::{Path, PathBuf};

pub fn file_list(dir: &Path) -> Result<Vec<PathBuf>, walkdir::Error> {
    let mut filenames = Vec::new();
    let dir_entries = WalkDir::new(dir);
    for entry in dir_entries {
        let dir_entry = entry?;
        if dir_entry
            .path()
            .extension()
            .map(|ext| ext.eq("log"))
            .unwrap_or(false) {
            filenames.push(dir_entry.path().to_path_buf());
        }
    }

    Ok(filenames)
}

#[derive(Debug, PartialEq)]
pub enum AggregationMessages {
    Aggregate(usize, ELBRecordAggregation),
    Next(usize),
}

#[derive(Debug, PartialEq)]
pub enum FileHandlingMessages {
    Filename(PathBuf),
    Done,
}

#[derive(Debug)]
enum FileHandlingErrors<'a> {
    FileReadError { path: &'a Path, err: io::Error },
    LineReadError {
        path: &'a Path,
        line_nums: Vec<usize>,
    },
}

#[derive(Debug)]
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
                    self.aggregate_file(filename.as_path());
                    let _ = aggregate_sender.send(AggregationMessages::Next(self.id));
                }
                Ok(FileHandlingMessages::Done) => break,
                Err(RecvTimeoutError::Timeout) => {
                    debug!("A timeout occurred while FileAggregator {} was waiting \
                    for a filename. This may mean that the controller is having a hard time \
                    keeping up. But in most cases it just means there are no more files \
                    to process and this FileAggregator is just waiting for the shut down \
                    message.",
                    self.id)
                }
                Err(RecvTimeoutError::Disconnected) => {
                    // If this channel has disconnected there's no guarantee the other
                    // channels are still connected. This is very bad and is going to produce
                    // bad aggregates. Therefore, we write to stderr so the message can be captured
                    // and we kill the thread.
                    let msg = format!("FileAggregator {} has disconnected from the controller.\
                            This is unrecoverable error and should be reported to the developers.",
                                      self.id);
                    println_stderr!("{}", msg);
                    panic!(msg);
                }
            }
        }

        let _ = aggregate_sender.send(AggregationMessages::Aggregate(self.num_raw_records,
                                                                     self.final_agg));
    }

    fn aggregate_file(&mut self, file_path: &Path) -> () {
        debug!("FileAggregator {} received filename {}.",
        self.id,
        file_path.display());
        match self.read_file(file_path) {
            Err(FileHandlingErrors::FileReadError { path, err }) => {
                println_stderr!("Failed to read file {} with error {}. ",
                                path.display(),
                                err)
            }
            Err(FileHandlingErrors::LineReadError { path, line_nums }) => {
                println_stderr!("Failed to read lines {:?} from file {}. ",
                                line_nums,
                                path.display())
            }
            Ok(()) => {}
        }
    }

    fn read_file<'a>(&mut self, path: &'a Path) -> Result<(), FileHandlingErrors<'a>> {
        debug!("Processing file {}.", path.display());
        match File::open(path) {
            Ok(file) => self.read_records(path, &file),
            Err(err) => {
                Err(FileHandlingErrors::FileReadError {
                    path: path,
                    err: err,
                })
            }
        }
    }

    fn read_records<'a>(&mut self,
                        path: &'a Path,
                        file: &File)
                        -> Result<(), FileHandlingErrors<'a>> {
        let mut bad_line_nums = Vec::new();
        let mut records_processed = 0;
        for (line_num, possible_record) in BufReader::new(file).lines().enumerate() {
            if let Ok(record) = possible_record {
                record_handling::try_parse_record(&record, &mut self.final_agg);
                records_processed += 1;
            } else {
                bad_line_nums.push(line_num);
            }
        }

        debug!("Found {} records in file {}.",
        records_processed,
        path.display());
        self.num_raw_records += records_processed;
        if bad_line_nums.is_empty() {
            Ok(())
        } else {
            Err(FileHandlingErrors::LineReadError {
                line_nums: bad_line_nums,
                path: path,
            })
        }
    }
}

#[cfg(test)]
mod file_aggregator_run_tests {

    use std::sync::mpsc;

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
        let mut file_aggregator = super::FileAggregator::new(0);


        let _ = file_aggregator.read_records(&path, &file);

        assert_eq!(file_aggregator.final_agg.len(),
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
        let mut file_aggregator = super::FileAggregator::new(0);

        let _ = file_aggregator.read_file(&log_path);

        assert_eq!(file_aggregator.num_raw_records, num_lines)
    }

    #[test]
    fn process_file_should_return_an_error_when_the_file_cannot_be_opened() {
        let log_path = Path::new("bad_filename");
        let mut file_aggregator = super::FileAggregator::new(0);

        let result = file_aggregator.read_file(&log_path);

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