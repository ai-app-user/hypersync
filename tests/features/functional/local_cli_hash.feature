Feature: Local CLI hash inventory
  Scenario: Hash inventory writes standard file hashes and finalized folder totals
    Given a source tree with entries:
      | type | path                  | content | mode | mtime_ns            |
      | file | alpha.txt             | alpha   | 0640 | 1700100000111111111 |
      | file | nested/beta.txt       | beta    | 0600 | 1700100000222222222 |
      | file | nested/child/gamma.md | gamma   | 0644 | 1700100000333333333 |
      | dir  | nested                |         | 0750 | 1700100000444444444 |
      | dir  | nested/child          |         | 0700 | 1700100000555555555 |
    When I hash the source tree as CSV with sha256
    Then the latest command should succeed
    And the hash output should match sha256 for:
      | path                  | content |
      | alpha.txt             | alpha   |
      | nested/beta.txt       | beta    |
      | nested/child/gamma.md | gamma   |
    And the hash output should contain folder metadata:
      | path         | flat_file_count | flat_logical_size_bytes |
      | nested       | 1               | 4                       |
      | nested/child | 1               | 5                       |

  Scenario: Hash inventory writes independent md5 block hashes
    Given a source tree with entries:
      | type | path            | content    | mode | mtime_ns            |
      | file | alpha.txt       | alphabet   | 0640 | 1700100000111111111 |
      | file | nested/beta.txt | 0123456789 | 0600 | 1700100000222222222 |
      | dir  | nested          |            | 0750 | 1700100000444444444 |
    When I hash the source tree as CSV with md5 block hashes of size 4
    Then the latest command should succeed
    And the hash output should contain md5 block hashes of size 4 for:
      | path            | content    |
      | alpha.txt       | alphabet   |
      | nested/beta.txt | 0123456789 |

  Scenario: Hash inventory writes fast xxh3 hashes
    Given a source tree with entries:
      | type | path            | content | mode | mtime_ns            |
      | file | alpha.txt       | alpha   | 0640 | 1700100000111111111 |
      | file | nested/beta.txt | beta    | 0600 | 1700100000222222222 |
      | dir  | nested          |         | 0750 | 1700100000444444444 |
    When I hash the source tree as CSV with xxh3_64
    Then the latest command should succeed
    And the hash output should contain xxh3_64 hashes for:
      | path            |
      | alpha.txt       |
      | nested/beta.txt |
