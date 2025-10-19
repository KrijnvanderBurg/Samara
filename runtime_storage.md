# Runtime Storage Strategy

## Overview

A stable, simple system for storing Samara runtime configurations with Git as the single source of truth. Teams manage runtime configs in their own Git repositories, which are validated and automatically synced to a central PostgreSQL database.

## Architecture

![Architecture Diagram](./runtime_storage.plantuml)

### Components

1. **Git Repositories** (per team) - Teams store `*runtime*.yaml` files
2. **CI/CD Pipeline** - Validates all runtime files on push
3. **Protected Branches** - dev/acc/main branches map to DEV/ACC/PROD environments
4. **Webhook → Azure Function** - Ingests configs to PostgreSQL on merge
5. **PostgreSQL Database** - Central storage, one DB per environment
6. **Config API** - Services query for runtime configs

## Data Flow

### 1. Development & Validation

```
Team pushes runtime configs (*runtime*.yaml)
  ↓
CI/CD pipeline validates each file against Samara schema
  ↓
Validation pass → allow merge | Validation fail → block merge
```

### 2. Ingestion (on merge to branch)

```
Webhook triggers Azure Function
  ↓
Parse payload (repo_id, branch)
  ↓
Fetch all *runtime*.yaml files from Git (current state)
  ↓
BEGIN TRANSACTION
  DELETE all runtimes for (repo_id, branch)
  INSERT all fetched runtime files
COMMIT
```

**Key principle**: Idempotent atomic replace. Git is always the source of truth.

## Database Schema

```sql
CREATE TABLE runtime_configs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  repo_id VARCHAR NOT NULL,
  branch VARCHAR NOT NULL,  -- dev/acc/main
  filepath VARCHAR NOT NULL,
  config JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  UNIQUE(repo_id, branch, filepath)
);

CREATE INDEX idx_runtime_configs_lookup 
  ON runtime_configs(repo_id, branch);
```

## Design Decisions

### ✅ Chosen Approach: Idempotent Replace

**Why:**
- **Simplicity**: No versioning, no tracking tables, no hash logic
- **Stability**: Self-healing - always converges to Git state
- **Reliability**: Duplicate/missed webhooks are harmless
- **Atomicity**: Transaction ensures readers never see partial state
- **Git is audit**: Full history already in Git, no need to duplicate

### ❌ Rejected Alternatives

**Versioning with config hashes:**
- Added complexity (tracking versions, status flags)
- Not needed - teams fix issues by pushing new commits

**Git commit SHA tracking:**
- More complex (tracking table, SHA extraction logic)
- Optimization not needed for expected scale
- Rollback not required - users push fixes

### Transaction Safety

PostgreSQL's `READ COMMITTED` isolation (default) ensures:
- Readers **never see uncommitted changes**
- During DELETE+INSERT transaction, readers see old data
- After COMMIT, readers see new data atomically
- **No risk of empty state during transition**

## API Endpoints

### Get Runtime Config
```
GET /runtimes/{repo_id}/{branch}/{filepath}
Returns: Runtime config JSON
```

### List All Runtimes for Repo
```
GET /runtimes/{repo_id}/{branch}
Returns: Array of runtime configs
```

## Operational Characteristics

### Idempotency
- Same webhook can be processed multiple times safely
- Always fetches current Git state, not webhook payload content
- Final database state matches Git state

### Failure Modes
- **Webhook missed**: Next push auto-corrects (self-healing)
- **Git fetch fails**: Function retries or fails (no partial state)
- **Transaction fails**: Rolls back, old data remains intact
- **Duplicate webhooks**: Process multiple times, same result

### Performance
- One Git fetch per webhook (acceptable for push frequency)
- One transaction per webhook (DELETE + multiple INSERTs)
- No complex queries or hash computations needed

## Setup Requirements

1. **Azure Function** with:
   - Git integration (fetch files from Azure DevOps)
   - PostgreSQL connection (Azure Key Vault for credentials)
   - Webhook endpoint exposed

2. **PostgreSQL** (one per environment):
   - Dev, Acceptance, Production databases
   - Managed Azure Database for PostgreSQL recommended

3. **Azure DevOps Webhooks**:
   - Configure "Code Pushed" webhook per branch
   - Point to corresponding environment's Azure Function

4. **CI/CD Pipeline**:
   - Samara schema validation step
   - Block merge on validation failure

## Future Enhancements (Optional)

- **Manual sync endpoint**: Force re-sync from Git if needed
- **Metrics**: Track webhook processing time, error rates
- **Alerting**: Notify on validation failures, ingestion errors
- **Caching**: Add Redis layer if query performance becomes an issue