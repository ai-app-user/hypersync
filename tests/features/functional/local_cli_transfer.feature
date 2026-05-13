Feature: Local CLI transfer
  Scenario: Scan, diff, sync, and clean diff local folders
    Given a source tree with entries:
      | type | path            | content          | mode | mtime_ns            |
      | file | alpha.txt       | alpha            | 0640 | 1700100000111111111 |
      | file | nested/beta.bin | 0123456789abcdef | 0600 | 1700100000222222222 |
      | dir  | nested          |                  | 0750 | 1700100000333333333 |
    And a local target directory
    When I scan the source tree via the CLI
    Then the latest command should succeed
    And the latest command should finish within 15.0 seconds
    And the scan output should contain 2 rows
    When I diff the source and target via the CLI
    Then the latest command should succeed
    And the latest command should report "new=2"
    And the diff output should contain 2 rows
    When I sync the source tree via the CLI
    Then the latest command should succeed
    And the latest transfer should report "transferred=2"
    And the target tree should match:
      | type | path            | content          | mode | mtime_ns            |
      | file | alpha.txt       | alpha            | 0640 | 1700100000111111111 |
      | file | nested/beta.bin | 0123456789abcdef | 0600 | 1700100000222222222 |
      | dir  | nested          |                  | 0750 | 1700100000333333333 |
    When I diff the source and target via the CLI
    Then the latest command should succeed
    And the latest command should report "same=2"
    And the latest command should report "changed=0"
    And the latest command should report "new=0"
    And the latest command should report "target_only=0"
    And the diff output should contain 2 rows

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

  Scenario: Small files are packed into fewer data chunks during sync
    Given a generated source tree with 256 files of 128 bytes each
    And a local target directory
    When I sync the source tree via the CLI
    Then the latest command should succeed
    And the latest transfer should report "transferred=256"
    And the latest transfer should send fewer chunks than files
    And the latest command should finish within 20.0 seconds
    And the target should contain 256 files
