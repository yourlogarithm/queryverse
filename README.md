# QueryVerse
## Overview

Natural Language Processing powered Web Seach Engine.
It leverages a stack consisting of:
- **gRPC** - Inter-service communication
- **MongoDB** - Database
- **Redis** - Caching
- **Qdrant** - Vector Search
- **Tempo** - Tracing
- **Grafana** - Monitoring
- **Text Embeddings Inference API** - Document Embeddings
- **ELK Stack** - Log management

To start the application, simply run:
```sh
docker compose up -d
```

In order for the crawling to be triggered, an initial manual crawl request must be sent:
```sh
grpcurl -plaintext -d '{"url": "https://en.wikipedia.org/wiki/Main_Page"}' \
    -proto proto/crawler.proto \
    localhost:50051 \
    crawler.Crawler/Crawl
```
