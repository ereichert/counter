use std::path::Path;
use std::fs::File;
use std::io::{BufRead, BufReader};
use walkdir;
use walkdir::{DirEntry, WalkDir};
use elp;
use {Aggregation, CounterError};
use std::collections::HashMap;
use record_handling;

/// A utility method for retrieving all of the paths to ELB log files in a directory.
///
/// dir: The directory from which the paths of the ELB log files will be procured.
pub fn file_list(dir: &Path) -> Result<Vec<DirEntry>, walkdir::Error> {
    let mut filenames = Vec::new();
    let dir_entries = WalkDir::new(dir);
    for entry in dir_entries {
        let dir_entry = entry?;
        if dir_entry.path().extension().map(|ext| ext.eq("log")).unwrap_or(false) {
            filenames.push(dir_entry);
        }
    }

    Ok(filenames)
}

/// Attempt to parse every ELB record in every file in `filenames` and pass the results to the
/// `record_handler`.
///
/// Each file will be opened and each line, which should represent a ELB record, will be passed
/// through the parser.
///
/// # Failures
///
/// All failures including file access, file read, and parsing failures are passed to the
/// `record_handler` as a `ParsingErrors`.
pub fn process_file(filename: &DirEntry) -> (usize, Aggregation) {
    debug!("Processing file {}.", filename.path().display());
    match File::open(filename.path()) {
        Ok(file) => {
            let aggregation_result = read_records(&file);
            debug!("Found {} records in file {}.",
                   aggregation_result.0,
                   filename.path().display());
            aggregation_result
        }

        Err(_) => {
            unimplemented!()
            //            record_handling::parsing_result_handler(CounterError::CouldNotOpenFile {
            //                path: format!("{}", filename.path().display())},
            //                HashMap::with_capacity(0))
        }
    }
}

pub fn read_records(file: &File) -> (usize, Aggregation) {
    let mut agg: Aggregation = HashMap::new();

    let mut file_record_count = 0;
    for possible_record in BufReader::new(file).lines() {
        file_record_count += 1;
        if let Ok(record) = possible_record {
            record_handling::handle_parsing_result(elp::parse_record(&record)
                                                       .map_err(CounterError::RecordParsingErrors),
                                                   &mut agg);
        } else {
            // record_handling::parsing_result_handler(CounterError::LineReadError, &mut agg)
        }
    }

    (file_record_count, agg)
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
        run_int_test_in_test_dir(|test_dir|{
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
        run_int_test_in_test_dir(|test_dir|{
            let files = super::file_list(Path::new(test_dir)).unwrap();

            assert_eq!(files.len(), 0)
        })
    }

    pub fn run_int_test_in_test_dir<T>(test: T) -> ()
        where T: FnOnce(&str) -> () + panic::UnwindSafe
    {
        creat_test_dir();
        let test_dir_path = create_empty_dir();

        let result = panic::catch_unwind(|| {
            test(&test_dir_path)
        });

        let _ = fs::remove_dir_all(&test_dir_path);

        if let Err(err) = result {
            panic::resume_unwind(err);
        }
    }

    pub const TEST_DIR: &'static str = "./int_tests";
    static TEST_DIR_SYNC: sync::Once = sync::ONCE_INIT;
    fn creat_test_dir() {
        TEST_DIR_SYNC.call_once(|| {
            let _ = fs::create_dir(TEST_DIR);
        });
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

            self.generator.as_mut().map(|mut g| g.next().unwrap()).unwrap()
        }
    }

    const NAME_GENERATOR: NameGenerator = NameGenerator {
        generator: None,
    };
}
