# README

These steps are necessary to get your application up and running.

1. open CMD bash in the same folder location ass app.py
2. Launch the service with cmd>: python app.py
3. Load a Browser window(Microsoft Edge) with local host 8081
4. Error logs will display in either screen.
5. Reports are written to the auditreports folder.

### What is this repository for?

- Version 1.2
- Send JSON packets to an api

### How do I get set up?

- See requirements.txt for a list of requirements
- Dependencies will include Python and many command line plug ins. See requirements.txt for more details
- API is setup for the templates in the adapter templates folder
- Database configuration - input files in Excel CSV format
- How to run tests - Load API end point and password, along with the CSV file to load and the Adapter of choice - you will need to program the adapter.

### Limitations?

- Insert is operational for Classifications, Projects
- Update is operational for Projects,
- Upsert is not yet operational

### Debugging?

- See files in the root such as debug_output.txt, ui_debug_output.txt,

### Reporting

- See auditreport folder for logs of results at adapter level, api responses, json packet in preparation to send
- To do UPSERT - check existence of a record –> PUT if exists or POST if not exists

### Architecture

- Running Flask as a back end, opens a port that can host User Interface on the nominated port
- Possible to run many Flasks ie

cmd: python app.py 8090

then in a new bash window...

cmd: python app.py 8091

This will allow two migrations to run at the same time

- app.py will run on this flask
  app.py handles a large JSON object and handles passing one record at a time to the nominated API endpoint. A JSON might look like this (debug_output.txt)
  {
  "recordCount": 3,
  "generatedAt": "2025-11-24T04:16:02+00:00",
  "adapter_key": "classifications",
  "records": [
  {
  "values": {
  "classificationType": 1,
  "dataVersion": 0,
  "deleted": false,
  "description": null,
  "name": "Queensland",
  "parentId": 10
  },
  "meta": {
  "rowIndex": 2,
  "name": "Queensland",
  "parent_id": "10",
  "description": "",
  "header": "TRUE",
  "adapter_name": "Classifications",
  "raw": "10,Queensland,,TRUE\r",
  "result": "Success",
  "message": ""
  }
  },
  {
  "values": {
  "classificationType": 2,
  "dataVersion": 0,
  "deleted": false,
  "description": null,
  "name": "Kensington",
  "parentId": 251
  },
  "meta": {
  "rowIndex": 3,
  "name": "Kensington",
  "parent_id": "251",
  "description": "",
  "header": "FALSE",
  "adapter_name": "Classifications",
  "raw": "251,Kensington,,FALSE\r",
  "result": "Success",
  "message": ""
  }
  },
  {
  "values": {
  "classificationType": 2,
  "dataVersion": 0,
  "deleted": false,
  "description": null,
  "name": "Aldgate",
  "parentId": 251
  },
  "meta": {
  "rowIndex": 4,
  "name": "Aldgate",
  "parent_id": "251",
  "description": "",
  "header": "FALSE",
  "adapter_name": "Classifications",
  "raw": "251,Aldgate,,FALSE\r",
  "result": "Success",
  "message": ""
  }
  }
  ]
  }
- This shows 3 classifications bundled together in one JSON object. App.py will call a specified "handler" which will then handle each packet sending it to the api.
  {
  "values": {
  "classificationType": 2,
  "dataVersion": 0,
  "deleted": false,
  "description": null,
  "name": "Aldgate",
  "parentId": 251
  },
  "meta": {
  "rowIndex": 4,
  "name": "Aldgate",
  "parent_id": "251",
  "description": "",
  "header": "FALSE",
  "adapter_name": "Classifications",
  "raw": "251,Aldgate,,FALSE\r",
  "result": "Success",
  "message": ""
  }
  }
- Items such as "meta" are not sent to the api, but are used in saving the api response which then enables report writing for this record. It can then be tracked if a packet send was a success or failure and for what reason.

Core components of the tool include:

templates/index.html - user interface - potentially hard coding of username and password for bulk testing - adding new adapter names or entities - reporting does not work in this location, see auditreports for the migration run reports
app.py - the main engine of the tool
/adapters \*.php - specific code can be modified client by client - for example...Once client may have extra columns for Users, these can be modified here - one client may handle addresses differently and require two columns to be joined. - client specific mapping can be added eg Projects can turn project groups into the correct format for import. - copy and paste if you need a new one

/handlers (Entity) - each JSON packet needs different pieces to be sent to the api - eg Classifications sends a values packet, while Projects has another layer of objects on the same level as values. - each handler runs differently to give the api exacly what it needs. - many adapters can use the one handler (Entity). Likely all Users adapters will simply use the same Users "handler"/Entity

/helpers - each handler will call on common components from helper files - these will assist with the loading of data, conversion to JSON, writing of logs and errors - end points are stored in helpers, should you need additional end points/Entities to appear here they are added to this file, but also index.html

cli_runner.py - bash command level migration capability (not yet written) - use this to send data from the command line without the user interface.

dispatcher.py - links the index.html with the execution of the correct handler/entity

Example of adding a new adapter eg - Teams Users Relationship that has never been written:

- The bulk of changes to add adapters and modify how they perform: use handlers and adapters
- add new adapter eg copy and paste users.php
- (automatically picked up for ui selection)
- add a new helper called TeamsUsersRelationships.py
- add the endpoint to helpers/endpoints.py
- add the handler to dispatcher.py
- index.py may need to add the handler to the list too.
- app.py likely will need no modification - make the changes needed in handler and php file.
