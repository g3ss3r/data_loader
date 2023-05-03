# data_loader

### Description
Allows to request URLs in multiple threads using a template, measure response speed, save the results to a log or database and generate reports.

### How to install
1. Clone repository from git: `git clone https://github.com/g3ss3r/data_loader`

2. Create virtual environment: `python -m venv data_loader`  

3. Activate virtual environment: `source data_loader/bin/activate`

4. Install necessary libs: `pip install -r requirements.txt`

5. Create folder for jobs files: `mkdir jobs`

6. Add job file into ./jobs/ (like sample_job.csv, with each entity in new line)

7. Fill .env file (take a look on **.env.exmple**)
- `DB_PG_*` - params for postgress database connection, make sense with `-d` usage
- `DL_API_KEY` - value that will replace `{api_key}` in the template
- `BATCH_SIZE` - the number of tasks that will be loaded into the feeder at the same time
- `WORKERS_COUNT` - default threads count, you can override it using `-w` the parameter
- `LIMIT_DEFAULT` - default value  `{limit}` that will replace `{limit}` in the template
- `LONG_RESPONSE` - value in seconds, requests with a response time greater than the specified one will be marked as slow using WARNING in the log
- `LOGS_FOLDER` - default path for logs, if the folder does not exist, it will be created
- `URL_*` - URL template, can be used placeholders:
  - `{entity}` - will be replaced with line from job file
  - `{api_key}` - will be replaced with `DL_API_KEY` value
  - `{limit}` - will be replaced with `LIMIT_DEFAULT` value

### Usage
1. ```loader.py -u URL_TEST -w 3 -j ./jobs/sample_job.csv```
- Create 3 workers, 
- Which will use template `URL_TEST` from the .etm file, 
- And for each request, `{entity}` in template will be replaced with row from `./jobs/sample_job.csv`.

2. ```loader.py -u URL_TEST -w 3 -j ./jobs/sample_job.csv -d some_report```
- Same but ...
- Save results in `some_report` table. If the table does not exist, it will be created.

### Reports
For report generating can be used `report.ipynb`.

Takes the path to the log file as input in `report_file` variable.

Provides:
- Log parsing results (row with data and skipped rows)
- Common text report
  - Responses count
  - Unique entities count
  - Avg. response time
  - Stats by response code
  - Stats by processes
- Charts
  - Requests per time
  - Avg. response time for all response codes
  - Avg. response time for responses with code 200
  - Avg. response time for responses with errors
  - Response time by status code boxplot
- And possibilities to export
  - Top slowest responses
  - Responses with warnings and errors