Feature: CLI performance smoke tests
  Scenario: Scanning a generated tree stays within a smoke budget
    Given a generated source tree with 128 files of 4096 bytes each
    When I scan the source tree via the CLI
    Then the latest command should succeed
    And the latest command should finish within 15.0 seconds
    And the scan output should contain 128 rows

  Scenario: Transferring a generated tree stays within a smoke budget
    Given a generated source tree with 64 files of 8192 bytes each
    And a local target directory
    When I transfer the source tree via the CLI with cache threshold 4096
    Then the latest command should succeed
    And the latest transfer should report "transferred=64"
    And the latest command should finish within 20.0 seconds
    And the measured throughput should be at least 0.02 MiB/s
    And the target should contain 64 files

  Scenario: Buffer pipeline benchmarks report sane throughput
    When I run buffer transport benchmarks:
      | transport | pattern    | transports | buffers_per_transport | buffer_size | pool_slots | generator_threads | sender_threads | receiver_threads | discarder_threads | shared_input |
      | none      | zero       | 2          | 64                    | 65536       | 16         | 1                 | 1              | 1                | 1                 |              |
      | none      | xoshiro256 | 2          | 64                    | 65536       | 16         | 1                 | 1              | 1                | 1                 |              |
      | unix      | xoshiro256 | 2          | 32                    | 65536       | 16         | 1                 | 1              | 1                | 1                 |              |
      | tcp       | xoshiro256 | 2          | 32                    | 65536       | 16         | 1                 | 1              | 1                | 1                 |              |
    Then all benchmark commands should succeed
    And each buffer transport benchmark should move all generated buffers
    And each benchmark should report at least 0.001 Gbit/s

  Scenario: Hash speed benchmarks report sane throughput
    When I run hash speed benchmarks:
      | hash    | threads | block_size | duration_seconds |
      | xxh64   | 1       | 65536      | 0.1              |
      | sha256  | 1       | 65536      | 0.1              |
    Then all benchmark commands should succeed
    And each benchmark should report at least 0.001 Gbit/s

  Scenario: Pipeline autoscale data benchmark starts tiny and completes
    Given a generated source tree with 128 files of 8192 bytes each
    When I run a pipeline autoscale data benchmark
    Then the latest command should succeed
    And the latest command should finish within 15.0 seconds
