# WellPing-EMA-Parser
Converts Stanford Communities Project EMA data from JSON to CSV

* parser.py: Custom functions to flatten and clean individual JSON responses
* ripper.py: Wraps functions from **parser.py**


At the command line: **python3.6 ripper.py { TARGET DIRECTORY }**

------------------------------

Before

- ROOT DIRECTORY
  - parser.py
  - ripper.py
  - TARGET DIRECTORY
    - { EMA-file.json }


After

- ROOT DIRECTORY
  - parser.py
  - ripper.py
  - TARGET DIRECTORY
    - 89_Participant-Files
      - { Individual CSVs for each participant }
    - 99_Composite-CSV
      - { EMA-file.csv }
    - { EMA-file.json }
