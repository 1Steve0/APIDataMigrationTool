# README

These steps are necessary to get your application up and running.

1. open CMD bash in the same folder location ass app.py
2. Launch the service with cmd>: python app.py
3. Load a Browser window(Microsoft Edge) with local host 8081
4. Error logs will display in either screen.
5. Reports are written to the Reports folder.

### What is this repository for?

- Quick summary
- Version 1.1
- Send JSON packets to an api

### How do I get set up?

- Firstly you must know your API you are sending to
- You must know the format the API needs and program this into your adapter
- Dependencies will include Python and many command line plug ins. See requirements.txt for more details
- Database configuration - input files in Excel CSV format
- How to run tests - Load API end point and password, along with the CSV file to load and the Adapter of choice - you will need to program the adapter.

### Limitations?

- Insert is operational
- I have not spent time on Update or Upsert - These are Untested and likely not working
- To do UPDATE – check existence of a record –> POST JSON if exists
- To do UPSERT - check existence of a record –> PUT if exists or POST if not exists
