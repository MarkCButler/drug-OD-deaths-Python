PRAGMA foreign_keys = ON;

CREATE TABLE states (
  State TEXT NOT NULL PRIMARY KEY,
  Name TEXT NOT NULL
);

CREATE TABLE death_counts (
  State TEXT NOT NULL,
  Year INTEGER NOT NULL,
  Month TEXT NOT NULL,
  Indicator TEXT NOT NULL,
  Value INTEGER NOT NULL,
  Label TEXT NOT NULL,
  PRIMARY KEY (State, Year, Month, Indicator),
  FOREIGN KEY (State)
    REFERENCES states (State)
);

CREATE TABLE populations (
  State TEXT NOT NULL,
  Year INTEGER NOT NULL,
  Population INTEGER NOT NULL,
  PRIMARY KEY (State, Year),
  FOREIGN KEY (State)
    REFERENCES states (State)
);