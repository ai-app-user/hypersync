Feature: Local CLI transfer
  Scenario: Loopback transfer preserves metadata and skips unchanged files
    Given a source tree with entries:
      | type | path            | content           | mode | mtime_ns            |
      | file | alpha.txt       | alpha             | 0640 | 1700100000111111111 |
      | file | nested/beta.bin | 0123456789abcdef  | 0600 | 1700100000222222222 |
      | dir  | nested          |                   | 0750 | 1700100000333333333 |
      | dir  | empty           |                   | 0711 | 1700100000444444444 |
      | dir  | empty/child     |                   | 0700 | 1700100000555555555 |
    And a local target directory
    When I transfer the source tree via the CLI twice
    Then the latest command should finish within 15.0 seconds
    And the first transfer should report "transferred=2"
    And the second transfer should report "skipped=2"
    And the target tree should match:
      | type | path            | content           | mode | mtime_ns            |
      | file | alpha.txt       | alpha             | 0640 | 1700100000111111111 |
      | file | nested/beta.bin | 0123456789abcdef  | 0600 | 1700100000222222222 |
      | dir  | nested          |                   | 0750 | 1700100000333333333 |
      | dir  | empty           |                   | 0711 | 1700100000444444444 |
      | dir  | empty/child     |                   | 0700 | 1700100000555555555 |
