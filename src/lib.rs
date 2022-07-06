use std::fmt::{self, Display, Formatter};

mod mysql;
mod types;

pub use mysql::MySqlStore;

#[derive(Clone, Debug)]
struct Query<'a> {
    table: &'a str,
    inner: QueryInner,
}

#[derive(Clone, Debug)]
enum QueryInner {
    Create {
        columns: Vec<String>,
        values: Vec<String>,
    },
    Delete {
        conditions: Conditions,
    },
    Insert {
        columns: Vec<String>,
        values: Vec<String>,
    },
    Select {
        columns: Vec<String>,
        conditions: Conditions,
    },
}

impl<'a> Query<'a> {
    pub fn new(table: &'a str, kind: QueryKind) -> Self {
        let inner = match kind {
            QueryKind::Create => QueryInner::Create {
                columns: Vec::new(),
                values: Vec::new(),
            },
            QueryKind::Delete => QueryInner::Delete {
                conditions: Conditions::default(),
            },
            QueryKind::Insert => QueryInner::Insert {
                columns: Vec::new(),
                values: Vec::new(),
            },
            QueryKind::Select => QueryInner::Select {
                columns: Vec::new(),
                conditions: Conditions::default(),
            },
        };

        Self { table, inner }
    }

    #[inline]
    pub fn kind(&self) -> QueryKind {
        match self.inner {
            QueryInner::Create {
                columns: _,
                values: _,
            } => QueryKind::Create,
            QueryInner::Delete { conditions: _ } => QueryKind::Delete,
            QueryInner::Insert {
                columns: _,
                values: _,
            } => QueryKind::Insert,
            QueryInner::Select {
                columns: _,
                conditions: _,
            } => QueryKind::Select,
        }
    }

    pub fn push(&mut self, key: String, value: String) {
        match &mut self.inner {
            QueryInner::Create { columns, values } => {
                columns.push(key);
                values.push(value);
            }
            QueryInner::Delete { conditions: _ } => {
                unreachable!()
            }
            QueryInner::Insert { columns, values } => {
                columns.push(key);
                values.push(value);
            }
            QueryInner::Select {
                columns,
                conditions: _,
            } => {
                columns.push(key);
            }
        }
    }

    pub fn push_condition(&mut self, condition: Condition) {
        match &mut self.inner {
            QueryInner::Create {
                columns: _,
                values: _,
            } => unreachable!(),
            QueryInner::Delete { conditions } => {
                conditions.push(condition);
            }
            QueryInner::Insert {
                columns: _,
                values: _,
            } => {
                unreachable!()
            }
            QueryInner::Select {
                columns: _,
                conditions,
            } => {
                conditions.push(condition);
            }
        }
    }
}

impl<'a> Display for Query<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match &self.inner {
            QueryInner::Create { columns, values } => write!(
                f,
                "CREATE TABLE IF NOT EXISTS {} ({})",
                self.table,
                columns
                    .iter()
                    .zip(values)
                    .map(|(column, value)| format!("{} {}", column, value))
                    .collect::<Vec<String>>()
                    .join(",")
            ),
            QueryInner::Delete { conditions } => {
                write!(f, "DELETE FROM {}{}", self.table, conditions)
            }
            QueryInner::Insert { columns, values } => write!(
                f,
                "INSERT INTO {} ({}) VALUES ({})",
                self.table,
                columns.join(","),
                values.join(",")
            ),
            QueryInner::Select {
                columns,
                conditions,
            } => write!(
                f,
                "SELECT {} FROM {}{}",
                columns.join(","),
                self.table,
                conditions
            ),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Conditions {
    conditions: Vec<Condition>,
}

impl Conditions {
    pub fn push(&mut self, value: Condition) {
        self.conditions.push(value);
    }
}

impl Display for Conditions {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if self.conditions.is_empty() {
            return Ok(());
        }

        write!(f, " WHERE {}", self.conditions[0])?;

        for condition in self.conditions.iter().skip(1) {
            write!(f, " AND {}", condition)?;
        }

        Ok(())
    }
}

/// A single sql condition. (e.g. id = 1)
#[derive(Clone, Debug)]
struct Condition {
    column: String,
    value: String,
    comparator: Comparator,
}

impl Condition {
    pub fn new(column: String, value: String, comparator: Comparator) -> Self {
        Self {
            column,
            value,
            comparator,
        }
    }
}

impl Display for Condition {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{} {} {}", self.column, self.comparator, self.value)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
enum Comparator {
    Eq,
}

impl Display for Comparator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let string = match self {
            Self::Eq => "=",
        };

        write!(f, "{}", string)
    }
}

#[derive(Debug)]
pub(crate) enum QueryKind {
    Create,
    Delete,
    Insert,
    Select,
}
