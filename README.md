# O.P.O.R.A.

**O**ptimization • **P**lanning • **A**ssessment • **R**econnaissance • **A**daptation

A local personal assistant with long-term memory that learns from your conversations.  
The agent doesn't just answer questions — it accumulates knowledge about you across 12 key life domains and gradually begins to help proactively: reminding, warning, giving personalized advice, and building scenarios based solely on your real data.

---

## Key Features

### 12 Domains of Long-Term Memory
- `identity` — personal data, biography, life context
- `tasks` — recurring tasks, deadlines, plan conflicts
- `goals` — long-term goals, aligning actions with intentions
- `finance` — income, expenses, limits, personal inflation
- `health` — medication intake, restrictions, prevention
- `people` — contacts, trust levels, promises
- `work` — work-life balance, job-related tasks
- `skills` — your skills that the agent relies on in its advice
- `food` — food stocks, expiration dates, shopping list
- `recipes` — favorite recipes and preferences
- `safety` — threats, risks, digital and physical security
- `general` — everything else that doesn't fit the other domains

Each domain is populated with facts from dialogues and cross-enriches the others: health affects tasks, finances affect travel plans, contacts intertwine with errands.

### Automatic Hypothesis Extraction
In the background, a local LLM analyzes conversation history and extracts short statement-hypotheses (`draft` status).  
Example: from the phrase "Tomorrow to the dentist at 15:00, need to buy painkillers" the agent will propose the hypothesis "User has a dentist appointment tomorrow, 15:00" in the `health` domain.

### Controlled Knowledge Verification
All hypotheses go through a transparent confirmation process:
- The user sees the original quote from the dialogue.
- Options: **confirm**, **reject**, **edit**, or send for **external verification** (with explicit permission).
- Manual selection of external model, web search toggling, preview and editing of the sent context are supported.
- All actions are logged in the `verification_actions` table (audit).

### Complete Privacy and Offline Operation
- Core logic runs on local LLMs (9–27B parameters, quantized GGUF).
- All data is stored in a local PostgreSQL database and the Qdrant vector store.
- External APIs (OpenAI, Perplexity, etc.) are only called on user demand and under user control.

### Console Interface
A CLI based on `prompt_toolkit` with history, syntax highlighting, and special commands for verification management.

---

## Architecture (Main Components)
- **emb-srv** — a dedicated embedding microservice (FastAPI + Qwen3-Embedding-4B GGUF).
- **main-srv** — the main server with the orchestrator, memory and verification managers, and the CLI.
- **llama.cpp** — built-in submodule for running local inference (llama-server).
- **Storages** — PostgreSQL (structured data, hypotheses, audit) and Qdrant (vector search).

---

## How the Agent Works (Pipeline)

1. **Conversation** — you chat with the agent through the CLI.
2. **Knowledge Extraction** — in the background, the local model extracts hypotheses from messages and assigns them to the 12 domains.
3. **Verification Offer** — when the orchestrator is idle, the agent notifies you about accumulated `draft` hypotheses.
4. **Verification** — you review each hypothesis together with its source quote and decide: confirm, reject, refine, or check via external AI (with control over the sent data).
5. **Memory Activation** — confirmed knowledge populates the domains. The agent starts using it for personalized assistance: reminders, budget analysis, plan conflict warnings, etc.

---

## Technology Stack

- **Language:** Python 3.13
- **LLM Runtime:** llama.cpp (fork, included as a submodule)
- **Models:** Qwen3.5-9B (generation), Qwen3-Embedding-4B (embeddings) — quantized GGUF, and others as available
- **Web Framework:** FastAPI (emb-srv)
- **Databases:** PostgreSQL + Qdrant
- **CLI:** prompt_toolkit
- **Infrastructure:** systemd units, Docker Compose (for databases), YAML configurations
