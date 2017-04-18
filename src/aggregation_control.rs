use std::sync::mpsc;
use file_handling::{AggregationMessages, FileHandlingMessages};
use walkdir::DirEntry;
use record_handling;
use std::collections::HashMap;
use FileAggregation;

pub struct AggregationController {
    agg_msg_receiver: mpsc::Receiver<AggregationMessages>,
    file_handling_msg_senders: Vec<mpsc::Sender<FileHandlingMessages>>,
}

impl AggregationController {
    pub fn new(agg_msg_rec: mpsc::Receiver<AggregationMessages>,
               file_handling_msg_senders: Vec<mpsc::Sender<FileHandlingMessages>>)
               -> AggregationController {

        AggregationController {
            agg_msg_receiver: agg_msg_rec,
            file_handling_msg_senders: file_handling_msg_senders,
        }
    }

    pub fn run_aggregation(&mut self, mut filenames: &mut Vec<DirEntry>) -> FileAggregation {
        let mut number_of_raw_records = 0;
        let mut remaining_workers = self.file_handling_msg_senders.len();
        let mut final_agg = HashMap::new();
        loop {
            match self.agg_msg_receiver.recv() {
                Ok(AggregationMessages::Next(sender_id)) => {
                    let sender = &self.file_handling_msg_senders[sender_id];
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
                    remaining_workers -= 1;
                    if remaining_workers == 0 {
                        break;
                    }
                }
                Err(_) => debug!("Received an error from one of the parsing workers."),
            }
        }

        FileAggregation {
            num_raw_records: number_of_raw_records,
            aggregation: final_agg,
        }
    }
}
