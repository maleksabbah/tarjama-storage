# tarjama-storage

File registry for the **Tarjama** Arabic ASR platform. It issues presigned upload/download URLs so clients transfer files directly to object storage (S3 / MinIO) without passing through the API, and tracks file metadata in the `storage_db`.

Built with FastAPI.

## Architecture

The service follows a clean, layered architecture where each layer has one responsibility and depends only on the layer beneath it:

- **Routes** — thin HTTP controllers. They translate between HTTP requests/responses and DTOs and call the service layer. No business logic lives here.
- **Services** — the business logic. Orchestrates the work, enforces rules, and coordinates repositories. Knows nothing about HTTP.
- **Repositories** — data access. Wraps the database (and external stores) behind a clean interface so the service layer never touches raw queries or clients directly.
- **Entities** — the domain / ORM models the repositories persist and return.
- **Dtos** — the request/response shapes exchanged at the API boundary, kept separate from internal entities.
- **Config** — wiring: database, Redis, Kafka, and other clients, plus environment configuration.

This separation keeps the HTTP layer swappable, the business logic testable in isolation, and the data layer free to change without touching the rest.

The repository layer wraps the S3/MinIO client (presigning, object operations) alongside the database, so the service layer treats object storage as just another repository.

Part of a multi-service system — see the [platform overview](https://github.com/maleksabbah/tarjama-docker) for the full architecture, pipeline flow, and the other services.
