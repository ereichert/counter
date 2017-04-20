use std::sync::mpsc;
use file_handling::{AggregationMessages, FileHandlingMessages};
use record_handling;
use std::collections::HashMap;
use FileAggregation;
use std::path::PathBuf;

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

    pub fn run_aggregation(&mut self, mut filenames: &mut Vec<PathBuf>) -> FileAggregation {
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

#[cfg(test)]
mod aggregation_controller_unit_tests {

    use std::sync::mpsc;
    use test_common;
    use std::path::PathBuf;
    use file_handling::{AggregationMessages, FileHandlingMessages};
    use std::collections::HashMap;

    #[test]
    fn run_aggregation_returns_when_all_of_the_file_handlers_have_sent_their_aggs() {
        let num_file_handlers = 10;
        let mut file_handler_receivers = Vec::new();
        let mut file_handler_senders = Vec::new();
        let (agg_sndr, agg_recv) = mpsc::channel();
        for _ in 0..num_file_handlers {
            let (sndr, recv) = mpsc::channel();
            file_handler_receivers.push(recv);
            file_handler_senders.push(sndr);
        }

        for _ in 0..num_file_handlers {
            let _ = agg_sndr.send(AggregationMessages::Aggregate(0, HashMap::new()));
        }
        let mut agg_ctrl = super::AggregationController::new(agg_recv, file_handler_senders);
        let file_agg = agg_ctrl.run_aggregation(&mut Vec::new());

        assert_eq!(file_agg.num_raw_records, 0);
        assert_eq!(file_agg.aggregation.len(), 0);
    }

    #[test]
    fn run_aggregation_sends_done_msg_to_the_correct_file_handler_when_there_are_no_filenames() {
        let num_file_handlers = 10;
        let file_handler_of_interest = 3;
        let mut files = Vec::new();
        let mut file_handler_receivers = Vec::new();
        let mut file_handler_senders = Vec::new();
        let (agg_sndr, agg_recv) = mpsc::channel();
        for _ in 0..num_file_handlers {
            let (sndr, recv) = mpsc::channel();
            file_handler_receivers.push(recv);
            file_handler_senders.push(sndr);
        }

        let _ = agg_sndr.send(AggregationMessages::Next(file_handler_of_interest));
        // Once all of the file_handlers are finished the main loop of the controller will shutdown.
        for _ in 0..num_file_handlers {
            let _ = agg_sndr.send(AggregationMessages::Aggregate(test_common::TEST_LOG_FILE_AGGS,
                                                                 HashMap::new()));
        }
        let mut agg_ctrl = super::AggregationController::new(agg_recv, file_handler_senders);
        agg_ctrl.run_aggregation(&mut files);
        let received_msg = file_handler_receivers[file_handler_of_interest].recv();

        assert_eq!(received_msg.unwrap(), FileHandlingMessages::Done);
    }

    #[test]
    fn run_aggregation_sends_done_msg_to_the_correct_file_handler_after_filenames_are_gone() {
        let num_file_handlers = 10;
        let file_handler_of_interest = 3;
        let num_files = 5;
        let test_file_path_buf = PathBuf::from(test_common::TEST_LOG_FILE);
        let mut files = Vec::new();
        for _ in 0..num_files {
            files.push(test_file_path_buf.clone())
        }
        let mut file_handler_receivers = Vec::new();
        let mut file_handler_senders = Vec::new();
        let (agg_sndr, agg_recv) = mpsc::channel();
        for _ in 0..num_file_handlers {
            let (sndr, recv) = mpsc::channel();
            file_handler_receivers.push(recv);
            file_handler_senders.push(sndr);
        }
        for _ in 0..num_files + 1 {
            let _ = agg_sndr.send(AggregationMessages::Next(file_handler_of_interest));
        }
        // Once all of the file_handlers are finished the main loop of the controller will shutdown.
        for _ in 0..num_file_handlers {
            let _ = agg_sndr.send(AggregationMessages::Aggregate(test_common::TEST_LOG_FILE_AGGS,
                                                                 HashMap::new()));
        }
        let mut agg_ctrl = super::AggregationController::new(agg_recv, file_handler_senders);
        agg_ctrl.run_aggregation(&mut files);
        for _ in 0..num_files {
            let received_msg = file_handler_receivers[file_handler_of_interest].recv();

            assert_eq!(received_msg.unwrap(),
                       FileHandlingMessages::Filename(test_file_path_buf.clone()));
        }
        let expected_done_msg = file_handler_receivers[file_handler_of_interest].recv();

        assert_eq!(expected_done_msg.unwrap(), FileHandlingMessages::Done);
    }

    #[test]
    fn run_aggregation_sends_a_filename_to_the_correct_file_handler_when_there_is_a_filename() {
        let num_file_handlers = 10;
        let file_handler_of_interest = 3;
        let test_file_path_buf = PathBuf::from(test_common::TEST_LOG_FILE);
        let mut files = vec![test_file_path_buf.clone()];
        let mut file_handler_receivers = Vec::new();
        let mut file_handler_senders = Vec::new();
        let (agg_sndr, agg_recv) = mpsc::channel();
        for _ in 0..num_file_handlers {
            let (sndr, recv) = mpsc::channel();
            file_handler_receivers.push(recv);
            file_handler_senders.push(sndr);
        }

        let _ = agg_sndr.send(AggregationMessages::Next(file_handler_of_interest));
        // Once all of the file_handlers are finished the main loop of the controller will shutdown.
        for _ in 0..num_file_handlers {
            let _ = agg_sndr.send(AggregationMessages::Aggregate(test_common::TEST_LOG_FILE_AGGS,
                                                                 HashMap::new()));
        }
        let mut agg_ctrl = super::AggregationController::new(agg_recv, file_handler_senders);
        agg_ctrl.run_aggregation(&mut files);
        let received_msg = file_handler_receivers[file_handler_of_interest].recv();

        assert_eq!(received_msg.unwrap(),
                   FileHandlingMessages::Filename(test_file_path_buf));
    }
}
