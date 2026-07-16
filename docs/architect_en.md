agent/
├── pyproject.toml               # Python project: dependencies, version
├── .gitignore                   # Ignored files
├── .gitmodules                  # Imported modules
│
├── emb-srv/                     # Embedding server
│    ├── .venv/                  # Python virtual environment
│    ├── configs/
│    │   ├── model_config.yaml   # Model parameters configuration
│    │   └── server_config.yaml  # Server network settings
│    │
│    ├── models/                 # LLM models (ignored by git)
│    │   └── Qwen3-Embedding-4B-Q4_K_M.gguf
│    │
│    ├── src/
│    │   └── main.py             # FastAPI application with /embed endpoint
│    │
│    ├── systemd/
│    │   └── embedding-server.service  # Systemd unit for auto-starting the service
│    │
│    └── requirements.txt        # .venv dependencies file
│
├── main-srv/                    # Main server
│    ├── .venv/                  # Python virtual environment
│    ├── configs/
│    │   ├── docker-compose.yaml # Docker Compose for PostgreSQL and Qdrant
│    │   ├── postgresql.conf     # PostgreSQL configuration
│    │   ├── pg_hba.conf         # PostgreSQL authentication rules
│    │   ├── qdrant_config.yaml  # Qdrant configuration
│    │   ├── postgres_db_config.yaml  # PostgreSQL configuration (database connection)
│    │   ├── model_routing.yaml  # LLM model routing configuration
│    │   └── emb_srv_config.yaml # Embedding server access settings
│    │
│    ├── llama.cpp/              # Submodule llama.cpp (fork)
│    │   ├── CMakeLists.txt
│    │   ├── Makefile
│    │   ├── build/              # Compiled binaries (ignored by git)
│    │   └── ...                 # llama.cpp sources
│    │
│    ├── logs/                   # Agent operation logs
│    │   └── opora_full.log      # Full log (DEBUG+)
│    │
│    ├── models/                 # LLM models (ignored by git)
│    │   ├── qwen3_5/
│    │   │   └── Qwen3.5-9B-Q4_K_M.gguf
│    │   └── qwen3_5-tokenizer/
│    │       └── tokenizer.json  # Tokenizer for Qwen3.5
│    │
│    ├── requirements.txt        # .venv dependencies file
│    │
│    ├── scripts/
│    │   ├── start-db.sh         # Script to start all databases
│    │   └── start_llama-server.sh # Script to start llama-server with Qwen3.5 model
│    │
│    └── src/                    # Python source code
│        ├── __init__.py
│        ├── main.py             # Entry point (agent startup)
│        ├── version.py          # Global version from pyproject.toml
│        │
│        ├── db_manager/         # Database management
│        │   ├── __init__.py
│        │   ├── db_manager.py   # PostgreSQL connection (uses postgres_db_config.yaml)
│        │   ├── qdrant_manager.py   # Qdrant vector DB manager (upsert, search, delete)
│        │   ├── qdrant_schema.py    # Payload schema for opora_db collection
│        │   └── migrations/     # Postgres migrations
│        │       ├── __init__.py
│        │       ├── pg_migration_manager.py   # Database migration manager
│        │       ├── V001_initial.sql          # Initial schema (main agent tables)
│        │       ├── V002_verification.sql     # Hypothesis verification subsystem
│        │       ├── V003_knowledge_graph.sql  # Memory pseudo‑graph subsystem (nodes, edges, revisions, prompts)
│        │       └── V004_retrieval.sql        # Preprocessing and retrieval subsystem (retrieval logs, routing context)
│        │
│        ├── dialog_services/    # Dialogue lifecycle management
│        │   ├── __init__.py
│        │   └── dialogue_manager.py # Dialogue manager (create/close, timeouts)
│        │
│        ├── interfaces/         # Interfaces
│        │   ├── __init__.py
│        │   └── console_interface.py # Console UI
│        │
│        ├── memory_service/     # Agent long‑term memory subsystem
│        │   ├── __init__.py 
│        │   ├── hypothesis_service.py   # Unified module for hypothesis management
│        │   ├── memory_composer.py      # Executes hypothesis extraction and domain assignment
│        │   ├── topic_composer.py       # Hypothesis topic classification
│        │   ├── verification_service.py # Hypothesis verification session management
│        │   ├── verification_composer.py # Executes hypothesis verification tasks
│        │   ├── graph_linker_composer.py   # Builds logical links within a topic (LLM)
│        │   ├── graph_merge_composer.py    # LLM‑based resolution of hypothesis‑node merges
│        │   ├── graph_node_sync.py         # Graph node synchronization with Qdrant
│        │   ├── graph_route_composer.py    # Deterministic routing and node creation
│        │   ├── graph_summarize_composer.py # Hierarchical node summarization (LLM)
│        │   ├── entity_binding_composer.py      # Incremental binding of fact‑nodes to entity‑aggregators
│        │   ├── entity_clustering_composer.py   # Batch clustering of fact‑nodes and creation of entity‑aggregators
│        │   └── form_composer.py                # LLM‑based classification of hypotheses by structural forms
│        │
│        ├── model_service/      # LLM access abstraction with routing
│        │   ├── __init__.py
│        │   ├── model_service.py    # Router: selects provider by model_name (uses model_routing.yaml)
│        │   └── providers/          # LLM provider implementations
│        │       ├── __init__.py
│        │       ├── base.py             # Abstract LLMProvider interface
│        │       ├── local_llama.py      # Provider for local llama‑server
│        │       └── external_dashscope.py # Provider for DashScope API (stub)
│        │
│        ├── orchestrator/       # Task orchestration core
│        │   ├── __init__.py
│        │   ├── orchestrator_entry.py # Entry point: task creation from external events
│        │   ├── orchestrator.py       # Background loop: task selection and dispatch
│        │   └── response_composer.py  # Final response generation via ModelService
│        │
│        ├── preprocessing/      # Query preprocessing subsystem (question_preprocessing pipeline)
│        │   ├── __init__.py
│        │   ├── pipeline.py             # Pre‑analysis stage orchestrator (routing, decomposition, retrieval)
│        │   ├── routing_composer.py     # question_routing step: LLM pre‑analysis for domain determination
│        │   ├── decomposition_composer.py # query_decomposition step: LLM splitting of complex queries into sub‑questions
│        │   └── retrieval_composer.py   # knowledge_retrieval step: hybrid search and context assembly from graph
│        │
│        ├── services/           # Auxiliary services
│        │   ├── __init__.py
│        │   ├── lifecycle_manager.py # Global agent lifecycle manager
│        │   ├── service_metrics.py   # Task/step status updates, metrics
│        │   ├── tokens_counter.py    # Token counting for Qwen models
│        │   ├── emb_service.py       # Graph node vectorization service (calls emb‑srv, creates tasks, syncs with Qdrant)
│        │   └── datetime_context.py  # Temporal context (date, time) generation for LLM prompts
│        │
│        └── session_services/   # Session management
│            ├── __init__.py
│            └── session_manager.py   # Session manager and actor_id binding
│
└── docs/                        # Documentation
    └── ...