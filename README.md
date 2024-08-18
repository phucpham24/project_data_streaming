Beginner Data Engineering Project - Stream Version

Project

This project simulates an e-commerce use case where we aim to attribute every product checkout to the first click that led to it. The main objectives are:

1. Enrich checkout data with user information.
2. Identify the click that led to a checkout based on the earliest click in the previous hour.
3. Log the attributed checkouts to a table.

Run on Codespaces

1. Create codespaces from the repository.
2. Start the project using `make run`.
3. Access Flink UI via the `ports` tab and check the running job.

Run Locally

Prerequisites

Install the following:

- Git
- Docker and Docker Compose
- psql

For Windows, set up WSL and an Ubuntu virtual machine.

Architecture

The pipeline architecture involves:

1. Application: Generates clicks and checkout event data.
2. Queue: Sends data to Kafka topics.
3. Stream Processing:
   - Store click data in cluster state.
   - Enrich checkout data with user information.
   - Join checkout data with click data.
4. Logging: Store the enriched and attributed checkout data in Postgres.
5. Monitoring: Use Prometheus and Grafana to monitor the pipeline.

Code Design

We use Apache Table API for:

1. Defining source systems.
2. Processing data (enriching and attributing checkouts).
3. Defining sink systems.

The main function runs the data processing job by creating sources, sinks, and processing logic.

Run Streaming Job

Clone the repository and start the job

- Flink UI: Check the running job at `http://localhost:8081/`.
- Grafana: Visualize metrics at `http://localhost:3000`.

Check Output

Open a Postgres terminal:

```
pgcli -h localhost -p 5432 -U postgres -d postgres
```

Query the attributed checkouts:

```sql
SELECT checkout_id, click_id, checkout_time, click_time, user_name FROM commerce.attributed_checkouts order by checkout_time desc limit 5;
```

Tear Down

Use `make down` to stop the containers.

Contributing

Contributions are welcome via issues or PRs.

References

- Apache Flink docs
- Flink Prometheus example project
