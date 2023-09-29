use std::fmt::Display;

#[derive(Debug)]
pub enum EngineError {
    CsvError(csv_async::Error),
    IoError(std::io::Error),
}

impl Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::CsvError(e) => writeln!(f, "CSV data reading error: {e:?}"),
            EngineError::IoError(e) => writeln!(f, "IO error: {e:?}"),
        }
    }
}

impl From<csv_async::Error> for EngineError {
    fn from(value: csv_async::Error) -> Self {
        Self::CsvError(value)
    }
}

impl From<std::io::Error> for EngineError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError(value)
    }
}
