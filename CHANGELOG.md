# Changelog
All notable changes to this project will be documented in this file.

## [1.1.0] - 2016-11-08

### Added
- Database#compute_stats
- Database#values
- Database#refresh
- Database#invalidate_search_path
- Database#set
- Database#{profile/profile_for} (jeremyevans)
- Database#except_strategy (jeremyevans)
- Support for Kerberizing impala-ruby (colinmarc)
- Support for QueryID from impala-ruby
- Support for Sequel::Mock
- Experimental support for progress

### Changed
- Change types of integer/biginteger to int/bigint
- Enable keepalive on connection to database
- Format of this [CHANGELOG](http://keepachangelog.com/en/0.3.0/)
- Updated thrift (colinmarc)
- Intersect uses INNER JOIN

## [1.0.1] - 2016-09-20

### Added
- rbhive adapter (jeremyevans)
- :empty_null=>:ruby option to csv_to_parquet extension, which can support quoted CSV cells (jeremyevans)

### Fixed
- Disconnect detection in impala and rbhive adapters (jeremyevans)
- :search_path option handling when using Sequel::SQL::AliasedExpressions (jeremyevans)
- Make implicit qualify return an SQL::Identifier if given an unqualified string (jeremyevans)
- Speed up multi_insert and import (jeremyevans)
- Optimize csv_to_parquet extension by not spawning shells or unnecessary processes (jeremyevans)

### Changed
- Transfer ownership of gem over to Outcomes Insights (aguynamedryan)

## [1.0.0] - 2015-12-04

### Added
- Initial public release
