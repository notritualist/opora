"""
main-srv/src/memory_service/hypothesis_service.py

Единый модуль работы с гипотезами долговременной памяти.

Обязанности:
- Константы лимитов токенов и настроек
- CRUD гипотез и журнала обработанных сообщений
- Форматирование сообщений в текст для LLM
- Чанкинг по токенам (защита от переполнения контекста)
- Парсинг JSON-ответа экстрактора с валидацией полей:
  * knowledge_source (user/agent/external) — источник знания

  Интеграция: через orchestrator_tasks → orchestrator_steps → llm_metrics → hypotheses.
"""

__version__ = "1.1.0"
__description__ = "Unified module for handling long-term memory hypotheses"


import json
import logging
from typing import List, Dict, Any, Tuple, Optional, Set
import psycopg2
from psycopg2.extras import RealDictCursor

from services.tokens_counter import count_tokens_qwen

logger = logging.getLogger(__name__)

# =============================================================================
# === КОНСТАНТЫ ===
# =============================================================================

# Имя промпта в orchestrator.prompts
EXTRACTION_PROMPT_NAME: str = "memory_hypothesis_extractor"

# Максимум токенов на один чанк (защита от переполнения контекста)
MAX_CHUNK_TOKENS: int = 32768

# Максимум сообщений, выбираемых за один цикл анализа
MAX_MESSAGES_PER_BATCH: int = 30

# Минимальный confidence для сохранения гипотезы
MIN_CONFIDENCE_THRESHOLD: float = 0.3

# Константы обработки топиков гипотез
TOPIC_CLASSIFICATION_PROMPT_NAME: str = "hypothesis_topic_classifier"
MAX_TOPIC_BATCH_TOKENS: int = 32768 # Лимит токенов на батч классификации
MAX_TOPICS_PER_BATCH: int = 30      # Максимум гипотез в одном батче

# =============================================================================
# === РЕПОЗИТОРИЙ: CRUD и выборки ===
# =============================================================================
def get_dialogue_messages(
    db_config: dict,
    dialogue_id: str
) -> List[Dict[str, Any]]:
    """
    Возвращает все сообщения конкретного диалога в хронологическом порядке.
    
    Args:
        db_config: параметры подключения
        dialogue_id: UUID диалога
        
    Returns:
        Список сообщений: id, actor_id, actor_type, row_text, timestamp
    """
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, actor_id, actor_type, row_text, timestamp
                FROM dialogs.row_messages
                WHERE dialogue_id = %s
                ORDER BY timestamp ASC
            """, (dialogue_id,))
            return [dict(row) for row in cur.fetchall()]


def get_unprocessed_dialogues(
    db_config: dict,
    limit: int = 5
) -> List[str]:
    """
    Возвращает список dialogue_id из закрытых диалогов, 
    у которых есть необработанные сообщения.
    
    Args:
        db_config: параметры подключения
        limit: максимум диалогов за выборку
        
    Returns:
        Список UUID диалогов
    """
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT d.id, d.last_activity_at
                FROM dialogs.dialogues d
                JOIN dialogs.row_messages rm ON rm.dialogue_id = d.id
                WHERE d.status = 'completed'
                  AND NOT EXISTS (
                      SELECT 1 FROM memory.message_analyses ma 
                      WHERE ma.message_id = rm.id
                  )
                ORDER BY d.last_activity_at ASC
                LIMIT %s
            """, (limit,))
            return [str(row[0]) for row in cur.fetchall()]


def get_unprocessed_closed_messages(
    db_config: dict,
    limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Возвращает сообщения из ЗАКРЫТЫХ диалогов, которые ещё не были проанализированы.
    Берём и сообщения пользователя, и сообщения агента (оба типа нужны для контекста).
    Сортируем по timestamp ASC — анализируем старые диалоги первыми.
    
    Args:
        db_config: параметры подключения
        limit: максимум сообщений за выборку
        
    Returns:
        Список: id, actor_id, actor_type, dialogue_id, row_text, timestamp
    """
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT rm.id, rm.actor_id, rm.actor_type, rm.dialogue_id, 
                       rm.row_text, rm.timestamp, d.session_id
                FROM dialogs.row_messages rm
                JOIN dialogs.dialogues d ON rm.dialogue_id = d.id
                WHERE d.status = 'completed'
                  AND NOT EXISTS (
                      SELECT 1 FROM memory.message_analyses ma 
                      WHERE ma.message_id = rm.id
                  )
                ORDER BY rm.timestamp ASC
                LIMIT %s
            """, (limit,))
            return [dict(row) for row in cur.fetchall()]


def get_already_processed_ids(
    db_config: dict,
    message_ids: List[str]
) -> Set[str]:
    """Возвращает множество ID уже обработанных сообщений (двойная проверка)."""
    if not message_ids:
        return set()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT message_id FROM memory.message_analyses
                WHERE message_id = ANY(%s::uuid[])
            """, (message_ids,))
            return {str(row[0]) for row in cur.fetchall()}


def get_active_domains_with_descriptions(db_config: dict) -> List[Dict[str, str]]:
    """Возвращает список активных доменов с описаниями для промпта."""
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT code, name, description 
                FROM memory.knowledge_domains 
                WHERE is_active = TRUE 
                ORDER BY code
            """)
            return [dict(row) for row in cur.fetchall()]


def get_extraction_prompt(db_config: dict) -> Optional[Dict[str, Any]]:
    """Возвращает активный промпт экстракции из orchestrator.prompts."""
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, text, params
                FROM orchestrator.prompts
                WHERE name = %s
                  AND status IN ('testing'::prompt_status, 'active'::prompt_status)
                ORDER BY created_at DESC
                LIMIT 1
            """, (EXTRACTION_PROMPT_NAME,))
            row = cur.fetchone()
            return dict(row) if row else None


def save_hypotheses(
    db_config: dict,
    hypotheses: List[Dict[str, Any]],
    orchestrator_step_id: str,
    prompt_id: str,
    agent_version: str,
    dialogue_id: Optional[str] = None
) -> int:
    """
    Сохраняет извлечённые гипотезы в memory.hypotheses.
    Все гипотезы привязаны к одному шагу оркестратора.
    
    Returns: количество сохранённых гипотез.
    """
    if not hypotheses:
        return 0
        
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            count = 0
            for h in hypotheses:
                status = 'needs_clarification' if h.get('clarification_question') else 'draft'
                cur.execute("""
                    INSERT INTO memory.hypotheses (
                        domain_code, knowledge_source, source_message_ids, hypothesis_text, confidence,
                        status, orchestrator_step_id, prompt_id, agent_version, dialogue_id
                    ) VALUES (
                        %s, %s::memory.knowledge_source, %s::uuid[], %s, %s, %s, %s, %s, %s, %s::uuid
                    )
                """, (
                    h.get('domain_code', 'general'),
                    h.get('knowledge_source', 'user'),
                    h.get('source_message_ids', []),
                    h['hypothesis_text'],
                    h.get('confidence', 0.5),
                    status,
                    orchestrator_step_id,
                    prompt_id,
                    agent_version,
                    dialogue_id  # НОВОЕ
                ))
                count += 1
            conn.commit()
            return count


def mark_messages_analyzed(
    db_config: dict,
    message_ids: List[str],
    hypotheses_count: int,
    tokens_used: int,
    orchestrator_step_id: str,
    llm_metric_id: Optional[str],
    prompt_id: str,
    agent_version: str
) -> None:
    """Помечает сообщения как проанализированные в memory.message_analyses."""
    if not message_ids:
        return
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            for msg_id in message_ids:
                cur.execute("""
                    INSERT INTO memory.message_analyses (
                        message_id, hypotheses_count, tokens_used,
                        orchestrator_step_id, llm_metric_id, prompt_id, agent_version
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (message_id) DO NOTHING
                """, (
                    msg_id, hypotheses_count, tokens_used,
                    orchestrator_step_id, llm_metric_id, prompt_id, agent_version
                ))
            conn.commit()


def get_unclassified_hypotheses(db_config: dict, limit: int = 100) -> List[Dict[str, Any]]:
    """Возвращает гипотезы со статусом 'draft' и topic_id IS NULL."""
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, hypothesis_text, domain_code, knowledge_source
                FROM memory.hypotheses
                WHERE status = 'draft'::memory.hypothesis_status
                  AND topic_id IS NULL
                ORDER BY created_at ASC
                LIMIT %s
            """, (limit,))
            return [dict(row) for row in cur.fetchall()]


def get_all_topics(db_config: dict) -> List[Dict[str, str]]:
    """Возвращает весь справочник тем (с описаниями) для промпта."""
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, name, description FROM memory.topics ORDER BY name")
            return [dict(row) for row in cur.fetchall()]


def assign_topics_to_hypotheses(db_config: dict, assignments: List[Dict[str, Optional[str]]]) -> int:
    """
    Обновляет topic_id и переводит статус в 'needs_clarification'.
    assignments: [{"hypothesis_id": "...", "topic_id": "..." или None}, ...]
    """
    if not assignments:
        return 0
    updated = 0
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            for a in assignments:
                hyp_id = a.get('hypothesis_id')
                topic_id = a.get('topic_id')
                if not hyp_id:
                    continue
                cur.execute("""
                    UPDATE memory.hypotheses
                    SET topic_id = %s::uuid,
                        status = 'needs_clarification'::memory.hypothesis_status,
                        updated_at = NOW()
                    WHERE id = %s::uuid AND status = 'draft'::memory.hypothesis_status
                """, (topic_id, hyp_id))
                updated += cur.rowcount
            conn.commit()
    return updated


def get_topic_classification_prompt(db_config: dict) -> Optional[Dict[str, Any]]:
    """Возвращает активный промпт классификатора тем."""
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, text, params FROM orchestrator.prompts
                WHERE name = %s AND status IN ('testing'::prompt_status, 'active'::prompt_status)
                ORDER BY created_at DESC LIMIT 1
            """, (TOPIC_CLASSIFICATION_PROMPT_NAME,))
            row = cur.fetchone()
            return dict(row) if row else None
        

# =============================================================================
# === ФОРМАТИРОВАНИЕ И ЧАНКИНГ ===
# =============================================================================
def format_messages_text(messages: List[Dict[str, Any]]) -> str:
    """
    Форматирует сообщения в читаемый текст для LLM.
    Учитывает и user, и system (агент) сообщения.
    UUID показываем ПОЛНОСТЬЮ — модель должна вернуть их в source_message_ids.
    
    Формат:
    [user <full-uuid>]: текст
    [system]: текст
    """
    lines = []
    for msg in messages:
        role = msg['actor_type']  # 'user', 'owner', 'system'
        msg_id = str(msg['id'])
        text = (msg.get('row_text') or "").strip()
        if not text:
            continue
        lines.append(f"[{msg_id}] ({role}): {text}")
    return "\n".join(lines)


def chunk_messages_by_tokens(
    messages: List[Dict[str, Any]]
) -> List[List[Dict[str, Any]]]:
    """
    Разбивает список сообщений на чанки по MAX_CHUNK_TOKENS.
    Каждое сообщение попадает в один чанк целиком (не режем внутри).
    Если одно сообщение больше лимита — помещаем его в отдельный чанк.
    
    Returns: список чанков (каждый чанк = список сообщений)
    """
    if not messages:
        return []
    
    chunks: List[List[Dict[str, Any]]] = []
    current_chunk: List[Dict[str, Any]] = []
    current_tokens: int = 0
    
    for msg in messages:
        text = (msg.get('row_text') or "")
        msg_tokens = count_tokens_qwen(text)
        
        if msg_tokens > MAX_CHUNK_TOKENS:
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = []
                current_tokens = 0
            chunks.append([msg])
            continue
        
        if current_tokens + msg_tokens > MAX_CHUNK_TOKENS and current_chunk:
            chunks.append(current_chunk)
            current_chunk = []
            current_tokens = 0
        
        current_chunk.append(msg)
        current_tokens += msg_tokens
    
    if current_chunk:
        chunks.append(current_chunk)
    
    logger.debug(f"Messages chunked: {len(messages)} → {len(chunks)} chunks")
    return chunks


def render_extraction_prompt(system_prompt_template: str, db_config: dict) -> str:
    """Подставляет активные домены С ОПИСАНИЯМИ в системный промпт экстракции."""
    domains = get_active_domains_with_descriptions(db_config)
    domains_lines = []
    for d in domains:
        desc = d.get('description') or d.get('name') or ''
        domains_lines.append(f"• \"{d['code']}\" — {desc}")
    domains_str = "\n".join(domains_lines)
    return system_prompt_template.replace("{domains}", domains_str)


def build_extraction_user_prompt(
    chunk_messages: List[Dict[str, Any]]
) -> Tuple[str, List[str]]:
    """
    Формирует user-промт для LLM с чанком сообщений.
    
    Returns: (текст промпта, список ID сообщений в чанке)
    """
    text = format_messages_text(chunk_messages)
    message_ids = [str(m['id']) for m in chunk_messages]
    
    user_prompt = (
        f"Проанализируй следующий фрагмент диалога и извлеки все значимые знания.\n\n"
        f"--- ФРАГМЕНТ ДИАЛОГА ({len(chunk_messages)} сообщений) ---\n"
        f"{text}\n"
        f"--- КОНЕЦ ФРАГМЕНТА ---\n\n"
        f"Верни JSON-массив гипотез согласно формату в системном промпте."
    )
    return user_prompt, message_ids


def parse_extraction_response(
    raw_response: str,
    chunk_message_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Парсит JSON-ответ экстрактора с защитой от сбоев.
    
    Защиты:
    1. Убирает markdown-обёртку (```json ... ```)
    2. Пытается извлечь JSON-массив через regex, если json.loads упал
    3. Валидирует каждый элемент массива
    4. Автоматически подставляет chunk_message_ids в source_message_ids
    5. Отбрасывает гипотезы с confidence < MIN_CONFIDENCE_THRESHOLD
    
    Returns: список валидных гипотез (может быть пустым)
    """
    if not raw_response or not raw_response.strip():
        logger.warning("Empty response from LLM")
        return []
    
    clean = raw_response.strip()
    data = None
    
    # === ЗАЩИТА 1: Убираем markdown-обёртку ===
    if clean.startswith("```"):
        lines = clean.split("\n")
        start = 1
        end = -1 if lines[-1].strip().startswith("```") else len(lines)
        clean = "\n".join(lines[start:end]).strip()
    
    # === ЗАЩИТА 2: Пробуем стандартный парсинг ===
    try:
        data = json.loads(clean)
    except json.JSONDecodeError:
        logger.debug("Standard json.loads failed, trying regex extraction")
    
    # === ЗАЩИТА 3: Если стандартный парсинг упал — ищем массив через regex ===
    if data is None:
        import re
        # Ищем самый внешний массив [...]
        match = re.search(r'\[[\s\S]*\]', clean)
        if match:
            try:
                data = json.loads(match.group())
                logger.debug("Successfully extracted JSON array via regex")
            except json.JSONDecodeError:
                logger.error(
                    f"Regex extraction also failed. "
                    f"Raw response (first 1000 chars): {clean[:1000]}"
                )
                return []
        else:
            logger.error(
                f"No JSON array found in response. "
                f"Raw response (first 1000 chars): {clean[:1000]}"
            )
            return []
    
    # === ЗАЩИТА 4: Валидация структуры ===
    if not isinstance(data, list):
        logger.warning(f"Expected list, got {type(data).__name__}")
        return []
    
    if len(data) == 0:
        logger.debug("LLM returned empty array (no hypotheses)")
        return []
    
    # === ЗАЩИТА 5: Валидация каждого элемента ===
    valid = []
    for i, item in enumerate(data):
        # Пропускаем не-словари
        if not isinstance(item, dict):
            logger.warning(f"Item {i} is not a dict: {type(item).__name__}")
            continue
        
        # Обязательное поле: hypothesis_text
        hypothesis_text = item.get('hypothesis_text')
        if not hypothesis_text or not isinstance(hypothesis_text, str):
            logger.debug(f"Item {i} skipped: missing/invalid hypothesis_text")
            continue
        
        hypothesis_text = hypothesis_text.strip()
        if len(hypothesis_text) < 3:
            logger.debug(f"Item {i} skipped: hypothesis_text too short")
            continue
        
        # Валидация confidence
        try:
            confidence = float(item.get('confidence', 0.0))
        except (TypeError, ValueError):
            logger.warning(f"Item {i} skipped: invalid confidence value")
            continue
        
        if confidence < MIN_CONFIDENCE_THRESHOLD:
            logger.debug(f"Item {i} skipped: confidence {confidence} < {MIN_CONFIDENCE_THRESHOLD}")
            continue
        
        if confidence > 1.0:
            confidence = 1.0
        
        # domain_code — опционально, дефолт 'general'
        domain_code = item.get('domain_code')
        if not isinstance(domain_code, str) or not domain_code.strip():
            domain_code = 'general'
        else:
            domain_code = domain_code.strip()
        
        # source_message_ids — если модель не заполнила, подставляем весь чанк
        raw_sources = item.get('source_message_ids')
        if not isinstance(raw_sources, list) or len(raw_sources) == 0:
            sources = chunk_message_ids
        else:
            # Валидация и нормализация каждого ID
            sources = []
            # Строим маппинг префикс → полный UUID для восстановления обрезанных
            prefix_map = {cid[:8]: cid for cid in chunk_message_ids}
            
            for s in raw_sources:
                s_str = str(s).strip()
                if not s_str:
                    continue
                
                # Проверяем, валидный ли это UUID (36 символов с дефисами)
                if len(s_str) == 36 and s_str.count('-') == 4:
                    sources.append(s_str)
                elif s_str in chunk_message_ids:
                    # Полный UUID без дефисов или совпадает точно
                    sources.append(s_str)
                elif s_str[:8] in prefix_map:
                    # Модель вернула искажённый/обрезанный UUID — восстанавливаем по первым 8 символам
                    sources.append(prefix_map[s_str[:8]])
                    logger.debug(
                        f"Restored malformed source_message_id: "
                        f"'{s_str[:16]}...' → {prefix_map[s_str[:8]][:8]}..."
                    )
                else:
                    # Неизвестный ID — пробуем найти по префиксу
                    matched = [cid for cid in chunk_message_ids if cid.startswith(s_str[:8])]
                    if len(matched) == 1:
                        sources.append(matched[0])
                        logger.debug(
                            f"Matched source_message_id by prefix: "
                            f"'{s_str[:16]}...' → {matched[0][:8]}..."
                        )
                    else:
                        logger.warning(
                            f"Cannot resolve source_message_id '{s_str[:16]}...', skipping"
                        )
            
            # Если после фильтрации пусто — подставляем весь чанк
            if not sources:
                sources = chunk_message_ids
        
        # knowledge_source — валидация источника
        valid_sources = {'user', 'agent', 'external'}
        knowledge_source = item.get('knowledge_source')
        if not isinstance(knowledge_source, str) or knowledge_source not in valid_sources:
            logger.debug(f"Item {i}: invalid knowledge_source '{knowledge_source}', defaulting to 'user'")
            knowledge_source = 'user'
        
        valid.append({
            'hypothesis_text': hypothesis_text,
            'domain_code': domain_code,
            'knowledge_source': knowledge_source,
            'confidence': confidence,
            'source_message_ids': sources,
        })
    
    logger.info(
        f"Parsed {len(valid)}/{len(data)} hypotheses from response "
        f"(rejected: {len(data) - len(valid)})"
    )
    return valid