# checksql
Utility to validate SQL queries against DB schema. It extracts SQL from known list of DB tables and validates it by creating View for "select" statements and procedure for PL/SQL and then evaluating creation errors.
SQL statements may be tested in DB schema which is differs from the one where it extracted from, this allows to test some user configurable SQL before upgrade to ensure it is compatible with new DB structure.
