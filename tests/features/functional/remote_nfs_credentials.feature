Feature: Remote NFS credentials
  Scenario: A non-root receiver can restore ownership through remote libnfs credentials
    Given a source tree with entries:
      | type | path        | content | mode | mtime_ns            |
      | file | alpha.txt   | alpha   | 0640 | 1700200000111111000 |
      | dir  | empty       |         | 0711 | 1700200000222222000 |
      | dir  | empty/child |         | 0700 | 1700200000333333000 |
    And an exported NFS target with remote credentials "uid=0&gid=0"
    When I transfer the source tree via the CLI
    Then the latest command should finish within 20.0 seconds
    And the latest transfer should report "transferred=1"
    And the target tree should match:
      | type | path        | content | mode | uid    | gid    | mtime_ns            |
      | file | alpha.txt   | alpha   | 0640 | source | source | 1700200000111111000 |
      | dir  | empty       |         | 0711 | source | source | 1700200000222222000 |
      | dir  | empty/child |         | 0700 | source | source | 1700200000333333000 |
