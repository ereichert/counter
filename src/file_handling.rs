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
/// If the user uses the
/// [AWS S3 sync tool](http://docs.aws.amazon.com/cli/latest/reference/s3/sync.html)
/// to download their AWS ELB logs to a local disk the files will be in a very specific directory
/// hierarchy.  This utility will read the paths of the files, recursively searching a root
/// specified by the user, and append the paths to the `Vec<DirEntry>`, also provided by the user.
///
/// dir: The directory from which the paths of the ELB log files will be procured.
///
/// filenames: A Vec<`DirEntry`> to which the paths of the ELB log files will be written.
pub fn file_list(dir: &Path, filenames: &mut Vec<DirEntry>) -> Result<usize, walkdir::Error> {
    let dir_entries = WalkDir::new(dir);
    for entry in dir_entries {
        if let Ok(dir_entry) = entry {
            if dir_entry.path().extension().map(|ext| ext.eq("log")).unwrap_or(false) {
                filenames.push(dir_entry);
            }
        }
    }
    Ok(filenames.len())
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
