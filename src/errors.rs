use cord_message::{errors::Error as MessageError, Message};
use error_chain::*;
use std::io::Error as IoError;
use tokio::sync::mpsc::error::{TryRecvError, TrySendError};

error_chain! {
    foreign_links {
        ChanRecv(TryRecvError);
        ChanSend(TrySendError<Message>);
        Io(IoError);
        Message(MessageError);
    }

    errors {
        OversizedData {
            description("Data length cannot be greater than a u32")
            display("Data length cannot be greater than a u32")
        }

        OversizedNamespace {
            description("Namespace length cannot be greater than a u16")
            display("Namespace length cannot be greater than a u16")
        }
    }
}
